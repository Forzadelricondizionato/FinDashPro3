import aiohttp
import asyncio
import numpy as np
import pandas as pd
from typing import Dict
import structlog
from fdp.core.rate_limiter import TokenBucketRateLimiter
from fdp.core.config import config

logger = structlog.get_logger()

class AdvancedSentimentAnalyzer:
    def __init__(self, rate_limiter: TokenBucketRateLimiter, finnhub_key: str):
        self.rate_limiter = rate_limiter
        self.finnhub_key = finnhub_key
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=15, connect=5, sock_read=10)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def analyze_comprehensive(self, ticker: str) -> Dict[str, any]:
        """Analyze news sentiment from multiple sources."""
        news = await self._fetch_news(ticker)
        social = await self._analyze_social_sentiment(ticker)
        
        composite = (news.get("sentiment", 0) * 0.4 + social.get("sentiment", 0) * 0.6)
        
        return {
            "news_sentiment": float(news.get("sentiment", 0)),
            "social_sentiment": float(social.get("sentiment", 0)),
            "composite_score": float(composite),
            "volume": int(news.get("volume", 0) + social.get("volume", 0))
        }
    
    async def _fetch_news(self, ticker: str) -> Dict:
        """Fetch news from Finnhub (free tier: 60 req/min)."""
        if not self.finnhub_key:
            return {"sentiment": 0, "volume": 0}
        
        await self.rate_limiter.acquire("finnhub", self.finnhub_key, config.rl_finnhub)
        
        today = pd.Timestamp.now().date()
        week_ago = today - pd.Timedelta(days=7)
        
        url = (f"https://finnhub.io/api/v1/company-news?"
               f"symbol={ticker}&from={week_ago}&to={today}&token={self.finnhub_key}")
        
        try:
            async with self.session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return {"sentiment": 0, "volume": 0}
                
                data = await resp.json()
                if not data:
                    return {"sentiment": 0, "volume": 0}
                
                sentiments = []
                for article in data[:20]:  # Limit to recent 20 articles
                    sentiment = article.get("sentiment", 0)
                    if sentiment == "positive":
                        sentiments.append(1)
                    elif sentiment == "negative":
                        sentiments.append(-1)
                    else:
                        sentiments.append(0)
                
                return {
                    "sentiment": float(np.mean(sentiments)) if sentiments else 0,
                    "volume": len(data)
                }
        except Exception as e:
            logger.error("news_fetch_failed", ticker=ticker, error=str(e))
            return {"sentiment": 0, "volume": 0}
    
    async def _analyze_social_sentiment(self, ticker: str) -> Dict:
        """Analyze social sentiment from Twitter/Reddit (GDELT fallback)."""
        gdelt_sentiment = await self._fetch_gdelt(ticker)
        return {
            "sentiment": gdelt_sentiment,
            "volume": 100  # Placeholder
        }
    
    async def _fetch_gdelt(self, ticker: str) -> float:
        """Fetch from GDELT 2.0 API (free, no key required)."""
        try:
            # GDELT Timeline API
            url = "https://api.gdeltproject.org/api/v2/tv/tv"
            params = {
                "query": f"{ticker} AND (stock OR market)",
                "mode": "TimelineVolRaw",
                "format": "json"
            }
            
            async with self.session.get(url, params=params, timeout=15) as resp:
                if resp.status != 200:
                    return 0.0
                
                data = await resp.json()
                if "timeline" not in data:
                    return 0.0
                
                # Simple sentiment proxy: high volume = high interest
                volumes = [item["value"] for item in data["timeline"][:10]]
                avg_volume = np.mean(volumes) if volumes else 0
                
                return min(1.0, avg_volume / 1000)  # Normalize
                
        except Exception as e:
            logger.error("gdelt_fetch_failed", ticker=ticker, error=str(e))
            return 0.0
