import asyncio
import aiohttp
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import re
import structlog
from urllib.parse import quote
from fdp.core.rate_limiter import TokenBucketRateLimiter
from fdp.core.circuit_breaker import CircuitBreaker
from fdp.core.config import config

logger = structlog.get_logger()

class MultiSourceMarketDataManager:
    def __init__(self, rate_limiter: TokenBucketRateLimiter, circuit_breaker: CircuitBreaker):
        self.rate_limiter = rate_limiter
        self.circuit_breaker = circuit_breaker
        self.session: Optional[aiohttp.ClientSession] = None
        self.providers = ["yahoo", "alpha", "tiingo", "finnhub", "polygon", "fmp"]
        self.redis = rate_limiter.redis
        self.semaphore = asyncio.Semaphore(5)  # Global semaphore
    
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    def _validate_ticker(self, symbol: str) -> str:
        """Sanitize and validate ticker."""
        if not isinstance(symbol, str) or len(symbol) > 20:
            raise ValueError(f"Invalid ticker format: {symbol}")
        
        # Remove dangerous characters
        sanitized = re.sub(r'[^A-Z0-9\.\-\_]', '', symbol.upper())
        
        # Path traversal prevention
        if '..' in sanitized or sanitized.startswith('/') or sanitized.startswith('\\'):
            raise ValueError(f"Path traversal detected in ticker: {sanitized}")
        
        # Redis key safety
        if ':' in sanitized or ' ' in sanitized:
            raise ValueError(f"Invalid characters in ticker: {sanitized}")
        
        return sanitized
    
    async def fetch_ohlcv(self, symbol: str, days: int = 365, interval: str = "1d", 
                         provider_order: List[str] = None) -> pd.DataFrame:
        """Fetch OHLCV with multi-provider fallback."""
        try:
            clean_symbol = self._validate_ticker(symbol)
        except ValueError as e:
            logger.error("ticker_validation_failed", symbol=symbol, error=str(e))
            return pd.DataFrame()
        
        cache_key = f"ohlcv:{clean_symbol}:{days}:{interval}"
        
        # Try cache
        try:
            cached = await self.redis.get(cache_key)
            if cached:
                return pd.read_json(cached)
        except Exception as e:
            logger.warning("cache_read_failed", symbol=clean_symbol, error=str(e))
        
        provider_order = provider_order or self.providers
        
        async def try_provider(provider: str) -> tuple[pd.DataFrame, str]:
            async with self.semaphore:
                try:
                    df = await self._fetch_from_provider(provider, clean_symbol, days, interval)
                    if not df.empty:
                        # Cache successful result
                        await self.redis.setex(cache_key, 86400, df.to_json())
                        await self.rate_limiter.record_spend(provider, 0.001)  # Minimal cost
                        logger.info("market_data_fetched", symbol=clean_symbol, source=provider, rows=len(df))
                        return df, provider
                except Exception as e:
                    logger.warning("market_data_source_failed", symbol=clean_symbol, source=provider, error=str(e))
            return pd.DataFrame(), provider
        
        # Try providers in parallel with priority
        tasks = [try_provider(p) for p in provider_order]
        for coro in asyncio.as_completed(tasks):
            df, provider = await coro
            if not df.empty:
                # Cancel remaining tasks
                for t in tasks:
                    t.cancel()
                return df
        
        logger.error("market_data_all_sources_failed", symbol=clean_symbol)
        return pd.DataFrame()
    
    async def _fetch_from_provider(self, provider: str, symbol: str, days: int, interval: str) -> pd.DataFrame:
        """Fetch from specific provider with circuit breaker."""
        try:
            if provider == "yahoo":
                return await self.circuit_breaker.call("yahoo", self._fetch_yahoo, symbol, days, interval)
            elif provider == "alpha":
                return await self.circuit_breaker.call("alpha", self._fetch_alpha_vantage, symbol, days)
            elif provider == "tiingo":
                return await self.circuit_breaker.call("tiingo", self._fetch_tiingo, symbol, days)
            elif provider == "finnhub":
                return await self.circuit_breaker.call("finnhub", self._fetch_finnhub, symbol, days)
            elif provider == "polygon":
                return await self.circuit_breaker.call("polygon", self._fetch_polygon, symbol, days)
            elif provider == "fmp":
                return await self.circuit_breaker.call("fmp", self._fetch_fmp, symbol, days)
        except CircuitOpenError:
            logger.warning("provider_circuit_open_skipping", provider=provider)
        except Exception as e:
            logger.error("provider_fetch_error", provider=provider, error=str(e))
        return pd.DataFrame()
    
    async def _fetch_yahoo(self, symbol: str, days: int, interval: str) -> pd.DataFrame:
        """Fetch from Yahoo Finance."""
        await self.rate_limiter.acquire("yahoo", "default", config.rl_yahoo)
        
        def _sync_fetch():
            ticker = yf.Ticker(symbol)
            ticker.session.headers.update({
                "User-Agent": "FinDashPro/3.1.1 (mailto:contact@example.com)"
            })
            return ticker.history(period=f"{days}d", interval=interval)
        
        loop = asyncio.get_event_loop()
        hist = await loop.run_in_executor(None, _sync_fetch)
        
        if hist.empty:
            return pd.DataFrame()
        
        hist = hist.reset_index()
        hist.columns = [col.lower() for col in hist.columns]
        hist["symbol"] = symbol
        hist["date"] = pd.to_datetime(hist["date"])
        return hist
    
    async def _fetch_alpha_vantage(self, symbol: str, days: int) -> pd.DataFrame:
        """Fetch from Alpha Vantage."""
        if not config.alpha_key:
            return pd.DataFrame()
        
        await self.rate_limiter.acquire("alpha", config.alpha_key, config.rl_alpha)
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": symbol,
            "apikey": config.alpha_key,
            "outputsize": "full"
        }
        
        async with self.session.get(url, params=params, timeout=30) as resp:
            if resp.status != 200:
                return pd.DataFrame()
            
            data = await resp.json()
            ts = data.get("Time Series (Daily)", {})
            
            if not ts:
                return pd.DataFrame()
            
            df = pd.DataFrame.from_dict(ts, orient="index")
            df.index = pd.to_datetime(df.index)
            df.columns = ["open", "high", "low", "close", "adjusted_close", "volume", "dividend", "split"]
            df["symbol"] = symbol
            
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            
            return df.dropna().tail(days).reset_index().rename(columns={"index": "date"})
    
    async def _fetch_tiingo(self, symbol: str, days: int) -> pd.DataFrame:
        """Fetch from Tiingo."""
        if not config.tiingo_key:
            return pd.DataFrame()
        
        await self.rate_limiter.acquire("tiingo", config.tiingo_key, config.rl_tiingo)
        url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        params = {
            "token": config.tiingo_key,
            "startDate": (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d"),
            "endDate": datetime.now().strftime("%Y-%m-%d")
        }
        
        async with self.session.get(url, params=params, timeout=15) as resp:
            if resp.status != 200:
                return pd.DataFrame()
            
            data = await resp.json()
            if not data:
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"])
            df["symbol"] = symbol
            return df
    
    async def _fetch_finnhub(self, symbol: str, days: int) -> pd.DataFrame:
        """Fetch from Finnhub."""
        if not config.finnhub_api_key:
            return pd.DataFrame()
        
        await self.rate_limiter.acquire("finnhub", config.finnhub_api_key, config.rl_finnhub)
        url = "https://finnhub.io/api/v1/stock/candle"
        params = {
            "symbol": symbol,
            "resolution": "D",
            "from": int((datetime.now() - timedelta(days=days)).timestamp()),
            "to": int(datetime.now().timestamp()),
            "token": config.finnhub_api_key
        }
        
        async with self.session.get(url, params=params, timeout=15) as resp:
            if resp.status != 200:
                return pd.DataFrame()
            
            data = await resp.json()
            if not data.get("t"):
                return pd.DataFrame()
            
            df = pd.DataFrame({
                "date": pd.to_datetime(data["t"], unit="s"),
                "open": data["o"],
                "high": data["h"],
                "low": data["l"],
                "close": data["c"],
                "volume": data["v"],
                "symbol": symbol
            })
            return df.dropna()
    
    async def _fetch_polygon(self, symbol: str, days: int) -> pd.DataFrame:
        """Fetch from Polygon."""
        if not config.polygon_key:
            return pd.DataFrame()
        
        await self.rate_limiter.acquire("polygon", config.polygon_key, config.rl_polygon)
        
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        url = f"https://api.polygon.io/v2/aggs/ticker/{quote(symbol)}/range/1/day/{start_date}/{end_date}"
        params = {"apiKey": config.polygon_key}
        
        async with self.session.get(url, params=params, timeout=15) as resp:
            if resp.status != 200:
                return pd.DataFrame()
            
            data = await resp.json()
            results = data.get("results", [])
            
            if not results:
                return pd.DataFrame()
            
            df = pd.DataFrame(results)
            df["date"] = pd.to_datetime(df["t"], unit="ms")
            df.rename(columns={
                "o": "open",
                "h": "high",
                "l": "low",
                "c": "close",
                "v": "volume"
            }, inplace=True)
            df["symbol"] = symbol
            
            return df[["date", "open", "high", "low", "close", "volume", "symbol"]]
    
    async def _fetch_fmp(self, symbol: str, days: int) -> pd.DataFrame:
        """Fetch from Financial Modeling Prep."""
        if not config.fmp_api_key:
            return pd.DataFrame()
        
        await self.rate_limiter.acquire("fmp", config.fmp_api_key, config.rl_fmp)
        
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{quote(symbol)}"
        params = {"apikey": config.fmp_api_key}
        
        async with self.session.get(url, params=params, timeout=15) as resp:
            if resp.status != 200:
                return pd.DataFrame()
            
            data = await resp.json()
            historical = data.get("historical", [])
            
            if not historical:
                return pd.DataFrame()
            
            df = pd.DataFrame(historical)
            df["date"] = pd.to_datetime(df["date"])
            df["symbol"] = symbol
            
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            
            return df.tail(days).reset_index(drop=True)
