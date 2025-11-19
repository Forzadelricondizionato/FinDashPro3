import json
import hashlib
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import redis.asyncio as redis
import structlog

logger = structlog.get_logger()

class ModelRegistry:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.model_prefix = "model:metadata:"
        self.performance_prefix = "model:performance:"
        self.model_ttl = 86400 * 30

    def _generate_model_key(self, ticker: str, version: str) -> str:
        unique_str = f"{ticker}:{version}:{datetime.now().isoformat()}"
        return hashlib.sha256(unique_str.encode()).hexdigest()[:16]

    async def register(self, ticker: str, model_data: Dict[str, Any], performance_metrics: Dict) -> str:
        model_key = self._generate_model_key(ticker, model_data.get("version", "v1"))
        metadata_key = f"{self.model_prefix}{model_key}"
        metadata = {
            "ticker": ticker,
            "model_key": model_key,
            "created_at": datetime.now().isoformat(),
            "features": model_data.get("features", []),
            "hyperparameters": model_data.get("hyperparameters", {}),
            "performance": performance_metrics,
            "version": model_data.get("version", "v1"),
            "status": "active"
        }
        pipe = self.redis.pipeline()
        pipe.setex(metadata_key, self.model_ttl, json.dumps(metadata))
        pipe.setex(f"{self.performance_prefix}{model_key}", self.model_ttl, json.dumps(performance_metrics))
        pipe.sadd(f"models:active:{ticker}", model_key)
        await pipe.execute()
        logger.info("model_registered", ticker=ticker, model_key=model_key)
        return model_key

    async def get_model(self, ticker: str, version: str = "latest") -> Optional[Dict]:
        if version == "latest":
            model_keys = await self.redis.smembers(f"models:active:{ticker}")
            if not model_keys:
                return None
            version = sorted(model_keys)[-1]
        data = await self.redis.get(f"{self.model_prefix}{version}")
        return json.loads(data) if data else None

    async def cleanup_old_models(self, ticker: str, keep_latest: int = 3):
        model_keys = await self.redis.smembers(f"models:active:{ticker}")
        if len(model_keys) <= keep_latest:
            return
        sorted_keys = sorted(model_keys)[:-keep_latest]
        pipe = self.redis.pipeline()
        for key in sorted_keys:
            pipe.delete(f"{self.model_prefix}{key}")
            pipe.delete(f"{self.performance_prefix}{key}")
            pipe.srem(f"models:active:{ticker}", key)
        await pipe.execute()
        logger.info("old_models_cleaned", ticker=ticker, removed=len(sorted_keys))

    async def get_model_performance(self, ticker: str, days: int = 30) -> Dict:
        model_keys = await self.redis.smembers(f"models:active:{ticker}")
        performances = []
        for key in model_keys:
            perf = await self.redis.get(f"{self.performance_prefix}{key}")
            if perf:
                performances.append(json.loads(perf))
        if not performances:
            return {"accuracy": 0.0, "sharpe": 0.0}
        return {
            "accuracy": np.mean([p.get("accuracy", 0) for p in performances]),
            "sharpe": np.mean([p.get("sharpe", 0) for p in performances]),
            "total_models": len(performances)
        }

class DriftMonitor:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.drift_threshold = config.ml_drift_threshold
        self.drift_key_prefix = "drift:"

    async def check(self, ticker: str, current_data: pd.DataFrame) -> Dict:
        reference_key = f"reference_data:{ticker}"
        reference_json = await self.redis.get(reference_key)
        if not reference_json:
            return {"drift_detected": False, "reason": "no_reference"}
        reference_data = pd.read_json(reference_json)
        drift_score = self._calculate_drift(reference_data, current_data)
        is_drift = drift_score > self.drift_threshold
        if is_drift:
            await self.redis.hset(f"{self.drift_key_prefix}{ticker}", mapping={
                "score": drift_score,
                "detected_at": datetime.now().isoformat(),
                "threshold": self.drift_threshold
            })
            await self.redis.expire(f"{self.drift_key_prefix}{ticker}", 86400)
        return {
            "drift_detected": is_drift,
            "drift_score": drift_score,
            "threshold": self.drift_threshold
        }

    def _calculate_drift(self, reference: pd.DataFrame, current: pd.DataFrame) -> float:
        from scipy.stats import entropy
        try:
            ref_dist = reference.mean().values
            curr_dist = current.mean().values
            return float(entropy(ref_dist, curr_dist))
        except:
            return 0.0

class MLMonitoring:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    async def generate_report(self, reference_data: pd.DataFrame, current_data: pd.DataFrame) -> Dict:
        try:
            from evidently import ColumnMapping
            from evidently.report import Report
            from evidently.metric_preset import DataDriftPreset
            column_mapping = ColumnMapping()
            report = Report(metrics=[DataDriftPreset()])
            report.run(reference_data=reference_data, current_data=current_data, column_mapping=column_mapping)
            return json.loads(report.json())
        except ImportError:
            logger.warning("evidently_not_installed")
            return {"drift": {"detected": False}}
