import redis.asyncio as redis
import json
from typing import Dict, Any, Optional
import joblib
from pathlib import Path
import structlog

logger = structlog.get_logger()

class ModelRegistry:
    """Model registry with Redis backend."""
    
    def __init__(self, redis_client: redis.Redis, model_dir: Path = Path("./data/models")):
        self.redis = redis_client
        self.model_dir = model_dir
        self.model_dir.mkdir(parents=True, exist_ok=True)
    
    async def register(self, ticker: str, model_data: Dict[str, Any]):
        """Register a model in the registry."""
        key = f"model:{ticker}"
        model_data['registered_at'] = joblib.time.time()
        
        await self.redis.set(key, json.dumps(model_data))
        logger.info("model_registered", ticker=ticker, path=model_data.get('model_path'))
    
    async def get(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get model metadata."""
        data = await self.redis.get(f"model:{ticker}")
        return json.loads(data) if data else None
    
    async def list_models(self) -> Dict[str, Dict]:
        """List all registered models."""
        keys = await self.redis.keys("model:*")
        models = {}
        for key in keys:
            ticker = key.split(":")[1]
            models[ticker] = await self.get(ticker)
        return models
    
    async def promote_model(self, ticker: str, environment: str = "production"):
        """Promote model to production."""
        model_data = await self.get(ticker)
        if model_data:
            model_data['environment'] = environment
            model_data['promoted_at'] = joblib.time.time()
            await self.redis.set(f"model:{ticker}:prod", json.dumps(model_data))
            logger.info("model_promoted", ticker=ticker, environment=environment)
    
    async def delete_model(self, ticker: str):
        """Delete model from registry."""
        await self.redis.delete(f"model:{ticker}", f"model:{ticker}:prod")
        logger.info("model_deleted", ticker=ticker)
