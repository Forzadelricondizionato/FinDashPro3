import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score, TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import f1_score
import joblib
from pathlib import Path

class StackingEnsemble:
    """Stacking ensemble with temporal validation."""
    
    def __init__(self, ticker: str, model_dir: Path = Path("./data/models")):
        self.ticker = ticker
        self.model_dir = model_dir / ticker
        self.model_dir.mkdir(parents=True, exist_ok=True)
        
        self.models = {
            "rf": RandomForestClassifier(n_estimators=200, random_state=42, n_jobs=-1),
            "gb": GradientBoostingClassifier(n_estimators=100, random_state=42),
            "meta": LogisticRegression(random_state=42, max_iter=500)
        }
        self.scaler = StandardScaler()
        self.is_trained = False
    
    def train(self, X: pd.DataFrame, y: pd.Series) -> dict:
        """Train ensemble with time series cross-validation."""
        if X.empty or y.empty:
            return {"status": "failed", "reason": "empty_data"}
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Time series split for cross-validation
        tscv = TimeSeriesSplit(n_splits=5)
        
        # Train base learners
        meta_features = np.zeros((X_scaled.shape[0], len(self.models) - 1))
        
        for idx, (name, model) in enumerate([(k, v) for k, v in self.models.items() if k != "meta"]):
            model.fit(X_scaled, y)
            
            # Generate meta-features (out-of-fold predictions)
            oof_predictions = np.zeros(X_scaled.shape[0])
            for train_idx, val_idx in tscv.split(X_scaled):
                X_train_fold, X_val_fold = X_scaled[train_idx], X_scaled[val_idx]
                y_train_fold = y.iloc[train_idx]
                
                fold_model = model.__class__(**model.get_params())
                fold_model.fit(X_train_fold, y_train_fold)
                oof_predictions[val_idx] = fold_model.predict_proba(X_val_fold)[:, 1]
            
            meta_features[:, idx] = oof_predictions
            
            # Save base model
            joblib.dump(model, self.model_dir / f"{name}_model.pkl")
        
        # Train meta-learner
        self.models["meta"].fit(meta_features, y)
        joblib.dump(self.models["meta"], self.model_dir / "meta_model.pkl")
        joblib.dump(self.scaler, self.model_dir / "scaler.pkl")
        
        # Cross-validation scores
        scores = {}
        for name, model in self.models.items():
            if name != "meta":
                cv_scores = cross_val_score(model, X_scaled, y, cv=tscv, scoring='f1')
                scores[name] = cv_scores.mean()
        
        # Meta-learner score
        meta_pred = self.models["meta"].predict(meta_features)
        scores["meta"] = f1_score(y, meta_pred)
        scores["ensemble"] = np.mean(list(scores.values()))
        
        self.is_trained = True
        
        return {
            "status": "success",
            "scores": scores,
            "model_path": str(self.model_dir)
        }
    
    def predict(self, X: pd.DataFrame) -> dict:
        """Make predictions with trained ensemble."""
        if not self.is_trained:
            # Try loading from disk
            try:
                self._load_models()
            except:
                return {"status": "failed", "reason": "model_not_trained"}
        
        X_scaled = self.scaler.transform(X)
        
        # Generate meta-features
        meta_features = np.zeros((X_scaled.shape[0], len(self.models) - 1))
        
        for idx, (name, model) in enumerate([(k, v) for k, v in self.models.items() if k != "meta"]):
            meta_features[:, idx] = model.predict_proba(X_scaled)[:, 1]
        
        # Meta-learner prediction
        meta_proba = self.models["meta"].predict_proba(meta_features)
        meta_pred = self.models["meta"].predict(meta_features)
        
        direction = 1 if meta_pred.mean() == 1 else 0
        
        return {
            "status": "success",
            "direction": direction,
            "probabilities": meta_proba[:, 1],
            "expected_return": meta_proba[:, 1].mean() * 0.05,
            "confidence": meta_proba[:, 1].max()
        }
    
    def _load_models(self):
        """Load models from disk."""
        self.scaler = joblib.load(self.model_dir / "scaler.pkl")
        for name in self.models:
            self.models[name] = joblib.load(self.model_dir / f"{name}_model.pkl")
        self.is_trained = True

