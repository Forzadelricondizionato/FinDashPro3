import pandas as pd
import numpy as np
from typing import Dict, Any
import structlog

logger = structlog.get_logger()

class DataQualityGatekeeper:
    def __init__(self):
        self.min_rows = 100
        self.max_nan_pct = 0.1
        self.min_price_variation = 0.001

    def validate_ohlcv(self, df: pd.DataFrame) -> Dict[str, bool]:
        if df.empty or len(df) < self.min_rows:
            return {"sufficient_rows": False, "no_gaps": False, "valid_prices": False}
        nan_pct = df.isnull().sum().sum() / (len(df) * len(df.columns))
        if nan_pct > self.max_nan_pct:
            df = df.ffill().bfill()
        price_variance = df['close'].pct_change().std()
        return {
            "sufficient_rows": len(df) >= self.min_rows,
            "no_gaps": nan_pct <= self.max_nan_pct,
            "valid_prices": price_variance >= self.min_price_variation
        }

    def validate_fundamentals(self, data: Dict[str, Any]) -> bool:
        required_fields = ["currentRatio", "debtEquityRatio", "roe", "roa"]
        return all(field in data for field in required_fields)

    def detect_outliers(self, df: pd.DataFrame, column: str = "close") -> pd.Series:
        q1 = df[column].quantile(0.25)
        q3 = df[column].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        return (df[column] < lower_bound) | (df[column] > upper_bound)
