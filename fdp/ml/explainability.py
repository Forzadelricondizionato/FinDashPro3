# fdp/ml/explainability.py
import shap
import json
import pandas as pd
import numpy as np
import pickle
from pathlib import Path

class SHAPExplainability:
    def __init__(self, model_dir: Path):
        self.model_dir = model_dir
        self.explainer = None
        self.expected_value = None
    
    def fit(self, X: pd.DataFrame, model):
        X_sample = X.sample(min(100, len(X)))
        self.explainer = shap.TreeExplainer(model)
        shap_values = self.explainer.shap_values(X_sample)
        
        importance = pd.DataFrame({
            "feature": X_sample.columns,
            "importance": np.abs(shap_values).mean(axis=0)
        }).sort_values("importance", ascending=False)
        
        importance.to_csv(self.model_dir / "shap_importance.csv", index=False)
        self.expected_value = self.explainer.expected_value
        
        with open(self.model_dir / "shap_explainer.pkl", "wb") as f:
            pickle.dump(self.explainer, f)
        
        return importance
    
    def explain_prediction(self, X: pd.DataFrame) -> dict:
        if self.explainer is None:
            with open(self.model_dir / "shap_explainer.pkl", "rb") as f:
                self.explainer = pickle.load(f)
        
        shap_values = self.explainer.shap_values(X)
        return {
            "expected_value": self.expected_value,
            "shap_values": shap_values.tolist(),
            "features": X.columns.tolist(),
            "feature_values": X.iloc[0].tolist()
        }
