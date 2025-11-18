import pandas as pd
import numpy as np
from typing import Dict, Any
import json
import asyncio
import structlog

logger = structlog.get_logger()

class MLMonitoring:
    """Monitor ML model performance and data quality."""
    
    def __init__(self, redis_client):
        self.redis = redis_client
    
    async def generate_report(self, reference_data: pd.DataFrame, current_data: pd.DataFrame) -> Dict[str, Any]:
        """Generate ML performance report."""
        metrics = {
            "timestamp": pd.Timestamp.now().isoformat(),
            "shape_comparison": {
                "reference": reference_data.shape,
                "current": current_data.shape
            },
            "data_quality": {},
            "statistical_summary": {}
        }
        
        # Data quality checks
        metrics["data_quality"] = {
            "missing_values_ref": reference_data.isnull().sum().to_dict(),
            "missing_values_cur": current_data.isnull().sum().to_dict(),
            "duplicates_ref": reference_data.duplicated().sum(),
            "duplicates_cur": current_data.duplicated().sum()
        }
        
        # Statistical summary
        for col in reference_data.columns[:10]:  # Limit to first 10 columns
            if col in current_data.columns:
                metrics["statistical_summary"][col] = {
                    "mean_ref": float(reference_data[col].mean()),
                    "mean_cur": float(current_data[col].mean()),
                    "std_ref": float(reference_data[col].std()),
                    "std_cur": float(current_data[col].std()),
                    "min_ref": float(reference_data[col].min()),
                    "min_cur": float(current_data[col].min()),
                    "max_ref": float(reference_data[col].max()),
                    "max_cur": float(current_data[col].max())
                }
        
        return metrics
    
    async def track_prediction(self, ticker: str, features: pd.DataFrame, prediction: Dict):
        """Track prediction for later evaluation."""
        key = f"predictions:{ticker}:{pd.Timestamp.now().date().isoformat()}"
        
        data = {
            "timestamp": pd.Timestamp.now().isoformat(),
            "features_shape": features.shape,
            "prediction": prediction,
            "feature_stats": features.describe().to_dict()
        }
        
        await self.redis.lpush(key, json.dumps(data))
        await self.redis.expire(key, 86400 * 30)  # Keep for 30 days
        
        # Keep only last 100 predictions
        await self.redis.ltrim(key, 0, 99)
    
    async def get_model_performance(self, ticker: str, days: int = 30) -> Dict:
        """Get model performance over time."""
        pattern = f"predictions:{ticker}:*"
        keys = await self.redis.keys(pattern)
        
        performance = {
            "ticker": ticker,
            "total_predictions": len(keys),
            "avg_confidence": 0,
            "predictions": []
        }
        
        if not keys:
            return performance
        
        total_confidence = 0
        for key in keys[:days]:
            predictions = await self.redis.lrange(key, 0, 99)
            for p in predictions:
                try:
                    data = json.loads(p)
                    performance["predictions"].append({
                        "date": key.split(":")[-1],
                        "confidence": data["prediction"].get("confidence", 0),
                        "direction": data["prediction"].get("direction", 0)
                    })
                    total_confidence += data["prediction"].get("confidence", 0)
                except:
                    continue
        
        if performance["predictions"]:
            performance["avg_confidence"] = total_confidence / len(performance["predictions"])
        
        return performance
