import hashlib
import json
from datetime import datetime, timedelta
import pandas as pd
import redis.asyncio as redis
import structlog

logger = structlog.get_logger()

class FeatureStore:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.ttl_features = 86400

    def _generate_feature_key(self, ticker: str, feature_set: str, date: str) -> str:
        unique_str = f"{ticker}:{feature_set}:{date}"
        return f"features:{hashlib.sha256(unique_str.encode()).hexdigest()[:12]}"

    async def store(self, ticker: str, feature_set: str, date: str, features: pd.DataFrame) -> str:
        key = self._generate_feature_key(ticker, feature_set, date)
        metadata = {
            "ticker": ticker,
            "feature_set": feature_set,
            "date": date,
            "created_at": datetime.now().isoformat(),
            "features_count": len(features.columns),
            "rows_count": len(features)
        }
        pipe = self.redis.pipeline()
        pipe.setex(f"{key}:data", self.ttl_features, features.to_json())
        pipe.setex(f"{key}:meta", self.ttl_features, json.dumps(metadata))
        pipe.sadd(f"features:sets:{ticker}", feature_set)
        await pipe.execute()
        logger.debug("features_stored", ticker=ticker, key=key)
        return key

    async def retrieve(self, ticker: str, feature_set: str, date: str) -> Optional[pd.DataFrame]:
        key = self._generate_feature_key(ticker, feature_set, date)
        drift_key = f"drift:{ticker}:{feature_set}"
        drift_status = await self.redis.get(drift_key)
        if drift_status == "invalidated":
            logger.warning("features_invalidated_by_drift", ticker=ticker)
            return None
        data = await self.redis.get(f"{key}:data")
        if data:
            try:
                return pd.read_json(data)
            except Exception as e:
                logger.error("features_corrupted", key=key, error=str(e))
                await self.redis.delete(f"{key}:data")
                return None
        return None

    async def invalidate_due_to_drift(self, ticker: str, feature_set: str):
        await self.redis.setex(f"drift:{ticker}:{feature_set}", 86400, "invalidated")
        logger.warning("features_invalidated", ticker=ticker, feature_set=feature_set)
