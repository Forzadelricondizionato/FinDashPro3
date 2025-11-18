import pandas as pd
import numpy as np
from scipy import stats
from typing import Dict, Optional
import json
import structlog

logger = structlog.get_logger()

class DriftMonitor:
    """Monitor data and model drift."""
    
    def __init__(self, redis_client, psi_threshold: float = 0.2, ks_threshold: float = 0.05):
        self.redis = redis_client
        self.psi_threshold = psi_threshold
        self.ks_threshold = ks_threshold
    
    async def check(self, ticker: str, new_data: pd.DataFrame) -> Dict[str, any]:
        """Check for data drift."""
        reference_key = f"drift:reference:{ticker}"
        reference_data = await self.redis.get(reference_key)
        
        if reference_data is None:
            # First time, set reference
            await self.redis.setex(reference_key, 86400 * 30, new_data.to_json())
            return {"drift_detected": False, "message": "reference_set"}
        
        try:
            reference_df = pd.read_json(reference_data)
            
            drift_results = {
                "drift_detected": False,
                "psi_scores": {},
                "ks_scores": {},
                "warnings": []
            }
            
            # Check each feature
            for column in new_data.columns:
                if column not in reference_df.columns:
                    continue
                
                ref_series = reference_df[column].dropna()
                new_series = new_data[column].dropna()
                
                if ref_series.empty or new_series.empty:
                    continue
                
                # PSI (Population Stability Index)
                psi = self._calculate_psi(ref_series, new_series)
                if psi > self.psi_threshold:
                    drift_results["drift_detected"] = True
                    drift_results["warnings"].append(f"PSI drift in {column}: {psi:.3f}")
                
                # Kolmogorov-Smirnov test
                ks_stat, ks_pvalue = stats.ks_2samp(ref_series, new_series)
                if ks_pvalue < self.ks_threshold:
                    drift_results["drift_detected"] = True
                    drift_results["warnings"].append(f"KS drift in {column}: p={ks_pvalue:.3f}")
                
                drift_results["psi_scores"][column] = psi
                drift_results["ks_scores"][column] = {"statistic": ks_stat, "pvalue": ks_pvalue}
            
            # Update reference if drift is small
            if not drift_results["drift_detected"]:
                await self.redis.setex(reference_key, 86400 * 30, new_data.to_json())
            
            return drift_results
            
        except Exception as e:
            logger.error("drift_check_failed", ticker=ticker, error=str(e))
            return {"drift_detected": False, "error": str(e)}
    
    def _calculate_psi(self, expected: pd.Series, actual: pd.Series, bins: int = 10) -> float:
        """Calculate Population Stability Index."""
        try:
            # Create quantile bins from expected
            breakpoints = np.percentile(expected, np.linspace(0, 100, bins + 1))
            
            # Calculate percentages
            expected_percents = np.histogram(expected, bins=breakpoints)[0] / len(expected)
            actual_percents = np.histogram(actual, bins=breakpoints)[0] / len(actual)
            
            # Replace zeros with small value
            expected_percents = np.maximum(expected_percents, 0.0001)
            actual_percents = np.maximum(actual_percents, 0.0001)
            
            # Calculate PSI
            psi = np.sum((actual_percents - expected_percents) * np.log(actual_percents / expected_percents))
            
            return float(psi)
        except:
            return 0.0
    
    async def generate_report(self, ticker: str) -> Dict:
        """Generate drift report for ticker."""
        drift_key = f"drift:history:{ticker}"
        history_raw = await self.redis.lrange(drift_key, 0, 99)
        
        history = [json.loads(item) for item in history_raw if item]
        
        return {
            "ticker": ticker,
            "total_checks": len(history),
            "drift_events": sum(1 for h in history if h.get("drift_detected")),
            "last_check": history[0] if history else None,
            "history": history[:10]
        }
