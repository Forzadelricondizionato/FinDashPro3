import asyncio
import concurrent.futures
from datetime import datetime
import pandas as pd
from typing import Dict, Any
from fdp.ml.stacking_ensemble import StackingEnsemble
import structlog

logger = structlog.get_logger()

class AsyncModelTrainer:
    def __init__(self, redis_client, max_workers: int = 3):
        self.redis = redis_client
        self.max_workers = max_workers
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.feature_store = None

    def set_feature_store(self, feature_store):
        self.feature_store = feature_store

    async def train_model(self, ticker: str, X_train: pd.DataFrame, y_train: pd.Series, feature_set: str = "default") -> Dict[str, Any]:
        loop = asyncio.get_event_loop()
        model = StackingEnsemble(ticker)
        result = await loop.run_in_executor(self.executor, model.train, X_train, y_train)
        if self.feature_store:
            await self.feature_store.store(ticker, feature_set, datetime.now().strftime("%Y-%m-%d"), X_train)
        await self.redis.setex(f"model:last_train:{ticker}", 86400, datetime.now().isoformat())
        logger.info("model_trained_async", ticker=ticker, accuracy=result.get("ensemble", {}).get("f1"))
        return result

    def shutdown(self):
        self.executor.shutdown(wait=True)
        logger.info("trainer_shutdown_complete")
