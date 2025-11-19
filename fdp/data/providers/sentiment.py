import asyncio
import aiohttp
import re
from typing import Dict, Any, List
import structlog
from urllib.parse import quote
from fdp.core.rate_limiter import TokenBucketRateLimiter
import feedparser

logger = structlog.get_logger()

class AdvancedSentimentAnalyzer:
    def __init__(self, rate_limiter: TokenBucketRateLimiter, finnhub_key: str):
        self.rate_limiter = rate_limiter
        self.finnhub_key = finnhub_key
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def analyze_comprehensive(self, ticker: str) -> Dict[str, Any]:
        news = await self._fetch_finnhub_news(ticker)
        social = await self._fetch_social_sentiment(ticker)
        composite_score = self._calculate_composite(news, social)
        return {
            "composite_score": composite_score,
            "news_sentiment": news.get("sentiment", 0),
            "social_sentiment": social.get("sentiment", 0),
            "news_count": news.get("count", 0)
        }

    async def _fetch_finnhub_news(self, ticker: str) -> Dict[str, Any]:
        if not self.finnhub_key:
            return {}
        await self.rate_limiter.acquire("finnhub", f"{ticker}_news", config.rl_finnhub)
        url = "https://finnhub.io/api/v1/company-news"
        params = {
            "symbol": ticker,
            "from": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
            "to": datetime.now().strftime("%Y-%m-%d"),
            "token": self.finnhub_key
        }
        async with self.session.get(url, params=params, timeout=15) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
            if not data:
                return {}
            sentiments = [self._analyze_text_sentiment(article.get("headline", "")) for article in data[:10]]
            avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
            return {"sentiment": avg_sentiment, "count": len(data)}

    async def _fetch_social_sentiment(self, ticker: str) -> Dict[str, Any]:
        if not self.finnhub_key:
            return {}
        await self.rate_limiter.acquire("finnhub", f"{ticker}_social", config.rl_finnhub)
        url = f"https://finnhub.io/api/v1/stock/social-sentiment"
        params = {"symbol": ticker, "token": self.finnhub_key}
        async with self.session.get(url, params=params, timeout=15) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
            reddit_sentiment = sum([item.get("sentiment", 0) for item in data.get("reddit", [])[:10]]) / 10 if data.get("reddit") else 0
            twitter_sentiment = sum([item.get("sentiment", 0) for item in data.get("twitter", [])[:10]]) / 10 if data.get("twitter") else 0
            return {"sentiment": (reddit_sentiment + twitter_sentiment) / 2}

    def _analyze_text_sentiment(self, text: str) -> float:
        positive_words = ["growth", "profit", "rise", "gain", "bullish", "strong", "buy", "up"]
        negative_words = ["loss", "fall", "drop", "bearish", "weak", "sell", "down", "crisis"]
        words = re.findall(r'\w+', text.lower())
        pos_count = sum(1 for word in words if word in positive_words)
        neg_count = sum(1 for word in words if word in negative_words)
        total = pos_count + neg_count
        return (pos_count - neg_count) / total if total > 0 else 0

    def _calculate_composite(self, news: Dict, social: Dict) -> float:
        weights = {"news": 0.6, "social": 0.4}
        return (news.get("sentiment", 0) * weights["news"] + social.get("sentiment", 0) * weights["social"]) / sum(weights.values())
