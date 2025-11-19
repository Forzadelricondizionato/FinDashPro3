# fdp/core/orchestrator.py
import asyncio
import pandas as pd
import asyncpg
import redis.asyncio as redis
from typing import List, Dict, Any
import hashlib
from datetime import datetime
import structlog
from fdp.data.providers.market_data import MultiSourceMarketDataManager
from fdp.data.providers.fundamentals import FundamentalsManager
from fdp.data.providers.sentiment import AdvancedSentimentAnalyzer
from fdp.ml.stacking_ensemble import StackingEnsemble
from fdp.ml.features import FeatureEngineering
from fdp.trading.position_sizer import KellyPositionSizer
from fdp.trading.risk_manager import RiskManager
from fdp.core.circuit_breaker import CircuitBreaker
from fdp.core.rate_limiter import TokenBucketRateLimiter
from fdp.trading.broker_adapter_enhanced import EnhancedOrder, get_broker_adapter
from fdp.notifications.manager import MultiChannelNotifier
from fastapi import FastAPI
import uvicorn

logger = structlog.get_logger()

class FinDashProOrchestrator:
    def __init__(self):
        self.config = None
        self.db_pool = None
        self.redis = None
        self.market_data = None
        self.fundamentals = None
        self.sentiment = None
        self.model = None
        self.feature_engineer = None
        self.position_sizer = None
        self.risk_manager = None
        self.circuit_breaker = None
        self.rate_limiter = None
        self.notifier = None
        self.broker = None
        self.running = False
        self.num_shards = 1
        self.shard_id = 0
        self.app = FastAPI()
        self._setup_health_endpoint()
    
    def _setup_health_endpoint(self):
        @self.app.get("/health")
        async def health():
            try:
                if not self.redis or not self.db_pool:
                    return {"status": "initializing"}, 503
                await self.redis.ping()
                await self.db_pool.fetchval("SELECT 1")
                return {"status": "healthy", "timestamp": datetime.now().isoformat()}
            except Exception as e:
                return {"status": "unhealthy", "error": str(e), "timestamp": datetime.now().isoformat()}, 503
        
        @self.app.get("/metrics")
        async def metrics():
            if self.rate_limiter:
                return await self.rate_limiter.get_metrics()
            return {"error": "Rate limiter not initialized"}
    
    async def start_api_server(self):
        config = uvicorn.Config(self.app, host="0.0.0.0", port=8000, log_level="warning")
        server = uvicorn.Server(config)
        await server.serve()
    
    async def init_db_pool(self):
        self.db_pool = await asyncpg.create_pool(self.config.database_url)
    
    async def close_db_pool(self):
        if self.db_pool:
            await self.db_pool.close()
    
    async def init_redis_streams(self):
        try:
            await self.redis.xgroup_create("signals:stream", "fdp_group", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
    
    async def load_ticker_universe(self) -> pd.DataFrame:
        query = "SELECT symbol, region, type FROM ticker_universe WHERE active = true"
        rows = await self.db_pool.fetch(query)
        df = pd.DataFrame([dict(r) for r in rows])
        
        if hasattr(self.config, "max_tickers") and self.config.max_tickers:
            df = df.head(self.config.max_tickers)
        
        df["shard"] = df["symbol"].apply(lambda x: int(hashlib.md5(x.encode()).hexdigest(), 16) % self.num_shards)
        return df[df["shard"] == self.shard_id]
    
    async def producer(self, universe: pd.DataFrame):
        for _, row in universe.iterrows():
            signal = {
                "symbol": row["symbol"],
                "region": row["region"],
                "type": row["type"],
                "timestamp": datetime.now().isoformat()
            }
            await self.redis.xadd("signals:stream", signal)
            await asyncio.sleep(0.1)
    
    async def consumer(self):
        while self.running:
            try:
                messages = await self.redis.xreadgroup(
                    "fdp_group", "consumer_1", {"signals:stream": ">"}, count=1, block=1000
                )
                for stream, msg_list in messages:
                    for msg_id, data in msg_list:
                        await self.process_ticker_safe(data[b"symbol"].decode(), data[b"region"].decode(), data[b"type"].decode())
                        await self.redis.xack("signals:stream", "fdp_group", msg_id)
            except Exception as e:
                logger.error("consumer_error", error=str(e))
                await asyncio.sleep(5)
    
    async def process_ticker_safe(self, symbol: str, region: str, asset_type: str) -> Dict[str, Any]:
        try:
            return await self.process_ticker(symbol, region, asset_type)
        except Exception as e:
            await self.notifier.send_alert(f"Failed to process {symbol}: {str(e)}")
            return {"status": "failed", "error": str(e)}
    
    async def process_ticker(self, symbol: str, region: str, asset_type: str) -> Dict[str, Any]:
        data = await self._fetch_data(symbol, region, asset_type)
        features = self.feature_engineer.engineer_features(data["ohlcv"], data["fundamentals"], data["sentiment"])
        prediction = self.model.predict(features[0])
        
        if prediction["confidence"][0] < self.config.min_confidence:
            return {"status": "ignored", "reason": "low_confidence"}
        
        position_size = self.position_sizer.calculate_position_size(
            win_probability=prediction["confidence"][0],
            win_loss_ratio=2.0,
            account_summary=await self.broker.get_account_summary()
        )
        
        order = EnhancedOrder(
            symbol=symbol,
            action="buy" if prediction["direction"][0] == 1 else "sell",
            quantity=int(position_size // data["ohlcv"]["close"].iloc[-1]),
            order_type="limit",
            limit_price=data["ohlcv"]["close"].iloc[-1]
        )
        
        risk_check = self.risk_manager.validate_order(order)
        if not risk_check["allowed"]:
            return {"status": "blocked", "reason": risk_check["reason"]}
        
        if self.config.execution_mode != "alert_only":
            order_id = await self.broker.place_order(order)
            await self.redis.lpush("signals:queue", str({
                "ticker": symbol,
                "action": order.action,
                "confidence": prediction["confidence"][0],
                "order_id": order_id
            }))
        
        await self.notifier.send_alert(f"Signal: {symbol} {order.action} @ {order.limit_price}")
        return {"status": "success", "ticker": symbol, "confidence": prediction["confidence"][0]}
    
    async def _fetch_data(self, symbol: str, region: str, asset_type: str):
        ohlcv = await self.circuit_breaker.call("market_data", lambda: self.market_data.fetch_ohlcv(symbol, region, asset_type))
        fundamentals = await self.circuit_breaker.call("fundamentals", lambda: self.fundamentals.get_latest(symbol))
        sentiment = await self.circuit_breaker.call("sentiment", lambda: self.sentiment.analyze_comprehensive(symbol))
        return {"ohlcv": ohlcv, "fundamentals": fundamentals, "sentiment": sentiment}
    
    async def run(self):
        self.running = True
        await asyncio.gather(
            self.start_api_server(),
            self.consumer()
        )
    
    async def stop(self):
        self.running = False
        await self.close_db_pool()
