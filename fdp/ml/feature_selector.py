import pandas as pd
import numpy as np
from typing import Dict, Any
from sklearn.feature_selection import SelectKBest, f_classif, mutual_info_classif
from sklearn.ensemble import RandomForestClassifier
import structlog

logger = structlog.get_logger()

class AdvancedFeatureSelector:
    def __init__(self):
        self.max_features = 50

    def select_features(self, X: pd.DataFrame, y: pd.Series, method: str = "ensemble") -> Any:
        if X.empty or y.empty:
            return type('', (), {'features': X})()
        if method == "ensemble":
            selected = self._ensemble_selection(X, y)
        elif method == "statistical":
            selected = self._statistical_selection(X, y)
        elif method == "random_forest":
            selected = self._random_forest_selection(X, y)
        else:
            selected = X.columns.tolist()
        return type('', (), {'features': X[selected]})()

    def _ensemble_selection(self, X: pd.DataFrame, y: pd.Series) -> list:
        rf_selected = self._random_forest_selection(X, y)
        mi_selected = self._mutual_info_selection(X, y)
        f_selected = self._statistical_selection(X, y)
        combined = set(rf_selected) | set(mi_selected) | set(f_selected)
        return list(combined)[:self.max_features]

    def _statistical_selection(self, X: pd.DataFrame, y: pd.Series) -> list:
        selector = SelectKBest(f_classif, k=min(self.max_features, X.shape[1]))
        selector.fit(X, y)
        return X.columns[selector.get_support()].tolist()

    def _mutual_info_selection(self, X: pd.DataFrame, y: pd.Series) -> list:
        mi = mutual_info_classif(X, y)
        indices = np.argsort(mi)[-self.max_features:]
        return X.columns[indices].tolist()

    def _random_forest_selection(self, X: pd.DataFrame, y: pd.Series) -> list:
        rf = RandomForestClassifier(n_estimators=100, random_state=42)
        rf.fit(X, y)
        importances = rf.feature_importances_
        indices = np.argsort(importances)[-self.max_features:]
        return X.columns[indices].tolist()
