import asyncio
import aiohttp
import pandas as pd
from typing import Dict, Any, Optional
import structlog
from fdp.core.rate_limiter import TokenBucketRateLimiter

logger = structlog.get_logger()

class FundamentalsManager:
    def __init__(self, rate_limiter: TokenBucketRateLimiter):
        self.rate_limiter = rate_limiter
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_latest(self, ticker: str) -> Dict[str, Any]:
        if not config.fmp_api_key:
            return {}
        await self.rate_limiter.acquire("fmp", ticker, config.rl_fmp)
        url = f"https://financialmodelingprep.com/api/v3/ratios/{ticker}"
        params = {"apikey": config.fmp_api_key, "limit": 1}
        async with self.session.get(url, params=params, timeout=15) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
            if not data:
                return {}
            return data[0]

    async def get_financial_statements(self, ticker: str) -> Dict[str, pd.DataFrame]:
        if not config.fmp_api_key:
            return {}
        await self.rate_limiter.acquire("fmp", f"{ticker}_statements", config.rl_fmp)
        urls = {
            "income": f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}",
            "balance": f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{ticker}",
            "cash": f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{ticker}"
        }
        results = {}
        for stmt_type, url in urls.items():
            async with self.session.get(url, params={"apikey": config.fmp_api_key, "limit": 4}, timeout=15) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data:
                        results[stmt_type] = pd.DataFrame(data)
        return results
