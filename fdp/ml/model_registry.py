import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
import pandas as pd
from datetime import datetime
import structlog

logger = structlog.get_logger()

class ModelRegistry:
    def __init__(self, redis_client):
        self.redis = redis_client
        mlflow.set_tracking_uri("http://mlflow:5000")
        mlflow.set_experiment("fdp-ml-max")
    
    def log_model(self, model, ticker, metrics, hyperparams):
        with mlflow.start_run(run_name=f"{ticker}_{datetime.now().isoformat()}"):
            mlflow.log_params(hyperparams)
            mlflow.log_metrics(metrics)
            mlflow.sklearn.log_model(model, f"model_{ticker}")
            logger.info("model_logged", ticker=ticker, metrics=metrics)
    
    def get_model(self, ticker):
        runs = mlflow.search_runs(filter_string=f"tags.ticker='{ticker}'", order_by=["start_time DESC"])
        if len(runs) > 0:
            run_id = runs.iloc[0].run_id
            return mlflow.sklearn.load_model(f"runs:/{run_id}/model_{ticker}")
        return self._fallback_model()
    
    def _fallback_model(self):
        return RandomForestClassifier(n_estimators=100, random_state=42)

