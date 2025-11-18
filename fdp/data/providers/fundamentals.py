import asyncio
import aiohttp
import pandas as pd
from typing import Dict, Optional
import json
import re
import structlog
from fdp.core.rate_limiter import TokenBucketRateLimiter
from fdp.core.config import config

logger = structlog.get_logger()

class FundamentalsManager:
    def __init__(self, rate_limiter: TokenBucketRateLimiter):
        self.rate_limiter = rate_limiter
        self.session: Optional[aiohttp.ClientSession] = None
        self.redis = rate_limiter.redis
    
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_cik_from_ticker(self, ticker: str) -> Optional[int]:
        """Get CIK from ticker using SEC API."""
        cache_key = f"cik:{ticker.upper()}"
        cached = await self.redis.get(cache_key)
        if cached:
            return int(cached)
        
        await self.rate_limiter.acquire("sec", "default", 10)
        
        try:
            url = "https://www.sec.gov/files/company_tickers.json"
            headers = {
                "User-Agent": "FinDashPro/3.1.1 (mailto:contact@example.com)",
                "Accept": "application/json"
            }
            
            async with self.session.get(url, headers=headers, timeout=15) as resp:
                if resp.status != 200:
                    return None
                
                data = await resp.json()
                for company in data.values():
                    if company["ticker"].upper() == ticker.upper():
                        cik = company["cik_str"]
                        await self.redis.setex(cache_key, 7776000, str(cik))
                        return cik
        except Exception as e:
            logger.error("cik_lookup_failed", ticker=ticker, error=str(e))
        
        return None
    
    async def get_latest(self, ticker: str) -> Dict[str, float]:
        """Get latest fundamentals with multi-source fallback."""
        cache_key = f"fundamentals:{ticker.upper()}"
        cached = await self.redis.get(cache_key)
        if cached:
            try:
                return json.loads(cached)
            except:
                pass
        
        # Try SEC EDGAR first (free, primary)
        data = await self._fetch_sec_edgar(ticker)
        if data.get("source") == "sec_edgar" and data.get("revenue", 0) > 0:
            await self.redis.setex(cache_key, 86400, json.dumps(data))
            return data
        
        # Fallback to Finnhub (premium)
        if config.finnhub_api_key:
            data = await self._fetch_finnhub(ticker)
            if data.get("source") == "finnhub_premium":
                await self.rate_limiter.record_spend("finnhub", 0.01)
                await self.redis.setex(cache_key, 86400, json.dumps(data))
                return data
        
        # Fallback to FMP (premium)
        if config.fmp_api_key:
            data = await self._fetch_fmp(ticker)
            if data.get("source") == "fmp_premium":
                await self.rate_limiter.record_spend("fmp", 0.005)
                await self.redis.setex(cache_key, 86400, json.dumps(data))
                return data
        
        # Final fallback
        logger.warning("using_default_fundamentals", ticker=ticker)
        return self._get_default_fundamentals()
    
    async def _fetch_sec_edgar(self, ticker: str) -> Dict[str, float]:
        """Fetch from SEC EDGAR API."""
        cik = await self.get_cik_from_ticker(ticker)
        if not cik:
            return self._get_default_fundamentals()
        
        await self.rate_limiter.acquire("sec", "default", 10)
        
        try:
            url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
            headers = {
                "User-Agent": "FinDashPro/3.1.1 (mailto:contact@example.com)",
                "Accept": "application/json"
            }
            
            async with self.session.get(url, headers=headers, timeout=20) as resp:
                if resp.status != 200:
                    return self._get_default_fundamentals()
                
                data = await resp.json()
                facts = data.get("facts", {}).get("us-gaap", {})
                
                fundamentals = {
                    "revenue": self._extract_latest(facts, "Revenues"),
                    "net_income": self._extract_latest(facts, "NetIncomeLoss"),
                    "total_assets": self._extract_latest(facts, "Assets"),
                    "total_equity": self._extract_latest(facts, "StockholdersEquity"),
                    "total_debt": self._extract_latest(facts, "LongTermDebt"),
                    "operating_cash_flow": self._extract_latest(facts, "NetCashProvidedByUsedInOperatingActivities"),
                    "current_assets": self._extract_latest(facts, "AssetsCurrent"),
                    "current_liabilities": self._extract_latest(facts, "LiabilitiesCurrent"),
                    "gross_profit": self._extract_latest(facts, "GrossProfit"),
                    "ebit": self._extract_latest(facts, "OperatingIncomeLoss"),
                    "interest_expense": self._extract_latest(facts, "InterestExpense"),
                    "source": "sec_edgar"
                }
                
                return self._calculate_ratios(fundamentals)
                
        except Exception as e:
            logger.error("sec_edgar_failed", ticker=ticker, error=str(e))
            return self._get_default_fundamentals()
    
    def _extract_latest(self, facts: Dict, concept: str) -> float:
        """Extract latest value from SEC EDGAR facts."""
        try:
            concept_data = facts.get(concept, {})
            units = concept_data.get("units", {})
            
            for unit_values in units.values():
                if unit_values:
                    latest = sorted(unit_values, key=lambda x: x.get("frame", ""), reverse=True)[0]
                    return float(latest["val"])
        except Exception as e:
            logger.debug("concept_extraction_failed", concept=concept, error=str(e))
            return 0.0
        
        return 0.0
    
    def _calculate_ratios(self, fundamentals: Dict) -> Dict[str, float]:
        """Calculate financial ratios."""
        try:
            revenue = fundamentals.get("revenue", 1)
            net_income = fundamentals.get("net_income", 0)
            equity = fundamentals.get("total_equity", 1)
            assets = fundamentals.get("total_assets", 1)
            total_debt = fundamentals.get("total_debt", 0)
            current_assets = fundamentals.get("current_assets", 0)
            current_liabilities = fundamentals.get("current_liabilities", 1)
            gross_profit = fundamentals.get("gross_profit", 0)
            ebit = fundamentals.get("ebit", 0)
            interest_expense = fundamentals.get("interest_expense", 0)
            
            ratios = {
                "revenue": revenue,
                "net_income": net_income,
                "roe": net_income / equity if equity > 0 else 0,
                "roa": net_income / assets if assets > 0 else 0,
                "debt_to_equity": total_debt / equity if equity > 0 else 0,
                "current_ratio": current_assets / current_liabilities if current_liabilities > 0 else 999,
                "net_margin": net_income / revenue,
                "gross_margin": gross_profit / revenue if revenue > 0 else 0,
                "operating_margin": ebit / revenue if revenue > 0 else 0,
                "interest_coverage": ebit / interest_expense if interest_expense > 0 else 999,
                "pe_ratio": 15.0,
                "market_cap": equity * 15.0,
                "source": "sec_edgar"
            }
            
            # Validate against configured thresholds
            if not self._validate_fundamentals(ratios):
                logger.warning("fundamentals_validation_failed", ratios=ratios)
                return self._get_default_fundamentals()
            
            return ratios
            
        except Exception as e:
            logger.error("ratio_calculation_failed", error=str(e))
            return self._get_default_fundamentals()
    
    def _validate_fundamentals(self, ratios: Dict) -> bool:
        """Validate fundamentals against configured thresholds."""
        try:
            return all([
                ratios["current_ratio"] >= config.min_current_ratio,
                ratios["debt_to_equity"] <= config.max_debt_to_equity,
                ratios["gross_margin"] >= config.min_gross_margin,
                ratios["operating_margin"] >= config.min_operating_margin,
                ratios["net_margin"] >= config.min_net_margin,
                ratios["roe"] >= config.min_roe,
                ratios["roa"] >= config.min_roa,
                ratios["interest_coverage"] >= config.min_interest_coverage
            ])
        except Exception as e:
            logger.error("fundamentals_validation_check_failed", error=str(e))
            return False
    
    async def _fetch_finnhub(self, ticker: str) -> Dict[str, float]:
        """Fetch from Finnhub."""
        if not config.finnhub_api_key:
            return self._get_default_fundamentals()
        
        await self.rate_limiter.acquire("finnhub", config.finnhub_api_key, config.rl_finnhub)
        url = f"https://finnhub.io/api/v1/stock/metric?symbol={ticker}&metric=all&token={config.finnhub_api_key}"
        
        try:
            async with self.session.get(url, timeout=15) as resp:
                if resp.status != 200:
                    return self._get_default_fundamentals()
                
                data = await resp.json()
                metric = data.get("metric", {})
                
                return {
                    "pe_ratio": metric.get("peNormalizedAnnual", 15.0),
                    "roe": metric.get("roeTTM", 0.10),
                    "roa": metric.get("roaTTM", 0.05),
                    "debt_to_equity": metric.get("totalDebt/totalEquityAnnual", 0.5),
                    "current_ratio": metric.get("currentRatioAnnual", 2.0),
                    "net_margin": metric.get("netMarginAnnual", 0.15),
                    "gross_margin": metric.get("grossMarginAnnual", 0.30),
                    "operating_margin": metric.get("operatingMarginAnnual", 0.10),
                    "interest_coverage": metric.get("interestCoverage", 5.0),
                    "market_cap": metric.get("marketCapitalization", 1e9),
                    "source": "finnhub_premium"
                }
        except Exception as e:
            logger.error("finnhub_fundamentals_failed", ticker=ticker, error=str(e))
            return self._get_default_fundamentals()
    
    async def _fetch_fmp(self, ticker: str) -> Dict[str, float]:
        """Fetch from Financial Modeling Prep."""
        if not config.fmp_api_key:
            return self._get_default_fundamentals()
        
        await self.rate_limiter.acquire("fmp", config.fmp_api_key, config.rl_fmp)
        url = f"https://financialmodelingprep.com/api/v3/ratios/{
