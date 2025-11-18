import pandas as pd
from sklearn.feature_selection import SelectKBest, f_classif, mutual_info_classif
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from typing import Dict, Any

class AdvancedFeatureSelector:
    """Advanced feature selection with multiple methods."""
    
    def select_features(self, X: pd.DataFrame, y: pd.Series, method: str = "ensemble") -> Dict[str, Any]:
        """Select features using specified method."""
        if X.empty or y.empty:
            return {"features": X, "selected_count": X.shape[1], "method": "none"}
        
        if method == "ensemble":
            return self._ensemble_selection(X, y)
        elif method == "kbest":
            return self._kbest_selection(X, y)
        elif method == "forest":
            return self._forest_selection(X, y)
        else:
            return {"features": X, "selected_count": X.shape[1], "method": "none"}
    
    def _ensemble_selection(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, Any]:
        """Ensemble feature selection."""
        # Method 1: KBest ANOVA
        k = min(20, X.shape[1])
        selector_kbest = SelectKBest(score_func=f_classif, k=k)
        X_kbest = selector_kbest.fit_transform(X, y)
        kbest_features = X.columns[selector_kbest.get_support()].tolist()
        
        # Method 2: Mutual Information
        selector_mi = SelectKBest(score_func=mutual_info_classif, k=k)
        X_mi = selector_mi.fit_transform(X, y)
        mi_features = X.columns[selector_mi.get_support()].tolist()
        
        # Intersection of selections
        selected_features = list(set(kbest_features) & set(mi_features))
        
        if not selected_features:
            selected_features = kbest_features
        
        return {
            "features": X[selected_features],
            "selected_count": len(selected_features),
            "method": "ensemble",
            "kbest_scores": dict(zip(X.columns, selector_kbest.scores_)),
            "mi_scores": dict(zip(X.columns, selector_mi.scores_))
        }
    
    def _kbest_selection(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, Any]:
        """KBest selection only."""
        k = min(20, X.shape[1])
        selector = SelectKBest(score_func=f_classif, k=k)
        X_selected = selector.fit_transform(X, y)
        selected_features = X.columns[selector.get_support()].tolist()
        
        return {
            "features": X[selected_features],
            "selected_count": len(selected_features),
            "method": "kbest",
            "scores": dict(zip(X.columns, selector.scores_))
        }
    
    def _forest_selection(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, Any]:
        """Random Forest feature importance."""
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        forest = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
        forest.fit(X_scaled, y)
        
        importances = forest.feature_importances_
        indices = np.argsort(importances)[::-1]
        selected_features = X.columns[indices[:20]].tolist()
        
        return {
            "features": X[selected_features],
            "selected_count": len(selected_features),
            "method": "forest",
            "importances": dict(zip(X.columns, importances))
        }
