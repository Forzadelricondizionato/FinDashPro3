# fdp/data/cache_manager.py
import redis.asyncio as redis
import json
import pickle
from typing import Any, Optional

class CacheManager:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.ttl_map = {
            "ohlcv": 86400,
            "fundamentals": 604800,
            "sentiment": 3600
        }
    
    async def get(self, key: str, data_type: str) -> Optional[Any]:
        cached = await self.redis.get(f"cache:{data_type}:{key}")
        if cached:
            if data_type in ["ohlcv"]:
                return pickle.loads(cached)
            return json.loads(cached)
        return None
    
    async def set(self, key: str, data_type: str, value: Any):
        ttl = self.ttl_map.get(data_type, 3600)
        if data_type in ["ohlcv"]:
            serialized = pickle.dumps(value)
        else:
            serialized = json.dumps(value)
        await self.redis.setex(f"cache:{data_type}:{key}", ttl, serialized)
    
    async def invalidate(self, pattern: str):
        keys = await self.redis.keys(f"cache:*{pattern}*")
        if keys:
            await self.redis.delete(*keys)
