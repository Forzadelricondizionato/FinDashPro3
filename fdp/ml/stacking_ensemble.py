# fdp/ml/stacking_ensemble.py
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import f1_score
import pickle
import json
from pathlib import Path
import shap

class StackingEnsemble:
    def __init__(self, model_dir: Path):
        self.model_dir = model_dir
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.base_models = [
            ("rf", RandomForestClassifier(n_estimators=100, random_state=42)),
            ("gb", GradientBoostingClassifier(n_estimators=50, random_state=42))
        ]
        self.meta_model = LogisticRegression()
        self.scaler = StandardScaler()
        self.feature_names = None
        self.shap_explainer = None
    
    def train(self, X: pd.DataFrame, y: pd.Series) -> dict:
        self.feature_names = X.columns.tolist()
        X_scaled = self.scaler.fit_transform(X)
        
        tscv = TimeSeriesSplit(n_splits=5)
        meta_features = np.zeros((len(X), len(self.base_models)))
        
        for train_idx, val_idx in tscv.split(X_scaled):
            X_train, X_val = X_scaled[train_idx], X_scaled[val_idx]
            y_train = y.iloc[train_idx]
            
            for i, (name, model) in enumerate(self.base_models):
                model.fit(X_train, y_train)
                meta_features[val_idx, i] = model.predict_proba(X_val)[:, 1]
        
        self.meta_model.fit(meta_features, y)
        
        for name, model in self.base_models:
            path = self.model_dir / f"{name}_model.pkl"
            with open(path, "wb") as f:
                pickle.dump(model, f)
        
        with open(self.model_dir / "meta_model.pkl", "wb") as f:
            pickle.dump(self.meta_model, f)
        
        with open(self.model_dir / "scaler.pkl", "wb") as f:
            pickle.dump(self.scaler, f)
        
        with open(self.model_dir / "features.json", "w") as f:
            json.dump(self.feature_names, f)
        
        self._calculate_shap_values(X_scaled)
        
        return self._calculate_metrics(X_scaled, y)
    
    def _calculate_shap_values(self, X: np.ndarray):
        X_sample = X[:100] if len(X) > 100 else X
        rf_model = [m for n, m in self.base_models if n == "rf"][0]
        self.shap_explainer = shap.TreeExplainer(rf_model)
        shap_values = self.shap_explainer.shap_values(X_sample)
        
        feature_importance = np.abs(shap_values).mean(axis=0)
        importance_dict = dict(zip(self.feature_names, feature_importance))
        
        with open(self.model_dir / "shap_importance.json", "w") as f:
            json.dump(importance_dict, f)
    
    def predict(self, X: pd.DataFrame) -> dict:
        X_scaled = self.scaler.transform(X)
        meta_features = np.column_stack([
            model.predict_proba(X_scaled)[:, 1] for _, model in self.base_models
        ])
        
        probas = self.meta_model.predict_proba(meta_features)
        direction = np.argmax(probas, axis=1)
        confidence = np.max(probas, axis=1)
        
        return {
            "direction": direction.tolist(),
            "probabilities": probas.tolist(),
            "confidence": confidence.tolist(),
            "expected_return": (direction - 1) * confidence
        }
    
    def _calculate_metrics(self, X: np.ndarray, y: pd.Series) -> dict:
        predictions = self.predict(pd.DataFrame(X, columns=self.feature_names))
        f1 = f1_score(y, predictions["direction"])
        return {"ensemble": {"f1": f1}}
