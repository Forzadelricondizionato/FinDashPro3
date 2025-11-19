import pandas as pd
import numpy as np
from typing import Dict, Any
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, AdaBoostClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import StandardScaler
import structlog

logger = structlog.get_logger()

class StackingEnsemble:
    def __init__(self, ticker: str):
        self.ticker = ticker
        self.models = {
            "rf": RandomForestClassifier(n_estimators=200, max_depth=10, random_state=42),
            "gb": GradientBoostingClassifier(n_estimators=150, learning_rate=0.05, random_state=42),
            "ada": AdaBoostClassifier(n_estimators=100, random_state=42),
            "svc": SVC(probability=True, kernel='rbf', gamma='scale')
        }
        self.meta_model = LogisticRegression(random_state=42)
        self.scaler = StandardScaler()

    def train(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, Any]:
        if X.empty or y.empty:
            return {"ensemble": {"accuracy": 0.0, "f1": 0.0}}
        X_scaled = self.scaler.fit_transform(X)
        meta_features = np.zeros((X.shape[0], len(self.models)))
        for idx, (name, model) in enumerate(self.models.items()):
            model.fit(X_scaled, y)
            scores = cross_val_score(model, X_scaled, y, cv=5, scoring='f1')
            logger.info("model_trained", model=name, f1=scores.mean())
            if hasattr(model, "predict_proba"):
                meta_features[:, idx] = model.predict_proba(X_scaled)[:, 1]
            else:
                meta_features[:, idx] = model.predict(X_scaled)
        self.meta_model.fit(meta_features, y)
        meta_scores = cross_val_score(self.meta_model, meta_features, y, cv=5, scoring='f1')
        logger.info("meta_model_trained", f1=meta_scores.mean())
        return {"ensemble": {"accuracy": meta_scores.mean(), "f1": meta_scores.mean()}}

    def predict(self, X: pd.DataFrame) -> Dict[str, Any]:
        if X.empty:
            return {"direction": 0, "probabilities": [], "expected_return": 0.0}
        X_scaled = self.scaler.transform(X)
        meta_features = np.zeros((X.shape[0], len(self.models)))
        for idx, (name, model) in enumerate(self.models.items()):
            if hasattr(model, "predict_proba"):
                meta_features[:, idx] = model.predict_proba(X_scaled)[:, 1]
            else:
                meta_features[:, idx] = model.predict(X_scaled)
        final_pred = self.meta_model.predict(meta_features)
        final_prob = self.meta_model.predict_proba(meta_features)[:, 1] if hasattr(self.meta_model, "predict_proba") else final_pred
        expected_return = np.mean(final_prob) * 0.02
        return {
            "direction": 1 if np.mean(final_pred) > 0.5 else 0,
            "probabilities": final_prob.tolist(),
            "expected_return": expected_return
        }
