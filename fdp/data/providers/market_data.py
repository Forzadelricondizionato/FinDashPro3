import asyncio
import aiohttp
import pandas as pd
import yfinance as yf
from typing import Optional, List, Dict
from datetime import datetime, timedelta
import structlog
from pydantic import BaseModel

logger = structlog.get_logger()

class TickerValidationError(Exception):
    pass

class MarketDataProvider(BaseModel):
    name: str
    rate_limit: int
    timeout: int = 30

class MultiSourceMarketDataManager:
    def __init__(self, rate_limiter, circuit_breaker):
        self.rate_limiter = rate_limiter
        self.circuit_breaker = circuit_breaker
        self.providers = {
            "polygon": MarketDataProvider(name="polygon", rate_limit=5),
            "tiingo": MarketDataProvider(name="tiingo", rate_limit=500),
            "yahoo": MarketDataProvider(name="yahoo", rate_limit=2000)
        }
    
    def _validate_ticker(self, ticker: str) -> str:
        if not ticker or len(ticker) > 20:
            raise TickerValidationError("Invalid ticker length")
        if "../" in ticker or ".." in ticker:
            raise TickerValidationError("Path traversal detected")
        if not ticker.replace("-", "").replace(".", "").isalnum():
            raise TickerValidationError("Invalid characters in ticker")
        return ticker.upper()
    
    async def fetch_ohlcv(self, ticker: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        ticker = self._validate_ticker(ticker)
        
        async def fetch_yahoo():
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(None, lambda: yf.download(ticker, start=start_date, end=end_date))
            if data.empty:
                raise ValueError("No data from Yahoo Finance")
            data.reset_index(inplace=True)
            return data
        
        return await self.circuit_breaker.call("yahoo", fetch_yahoo)
    
    async def fetch_fundamentals(self, ticker: str) -> Dict:
        ticker = self._validate_ticker(ticker)
        
        async def fetch_finnhub():
            async with aiohttp.ClientSession() as session:
                url = f"https://finnhub.io/api/v1/stock/metric?symbol={ticker}&metric=all"
                headers = {"X-Finnhub-Token": self.rate_limiter.vault_client.read_secret("finnhub_key")}
                async with session.get(url, headers=headers, timeout=self.providers["tiingo"].timeout) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("metric", {})
                    raise ValueError(f"Finnhub error: {resp.status}")
        
        return await self.circuit_breaker.call("finnhub", fetch_finnhub)
