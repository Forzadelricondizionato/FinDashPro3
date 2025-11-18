import pandas as pd
import numpy as np
from typing import Dict

class DataQualityGatekeeper:
    """Validate market data quality."""
    
    def validate_ohlcv(self, df: pd.DataFrame) -> Dict[str, bool]:
        """Validate OHLCV data integrity."""
        if df.empty:
            return {"non_empty": False}
        
        # Ensure required columns exist
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_columns):
            return {"has_required_columns": False}
        
        # Basic checks
        checks = {
            "non_empty": len(df) > 100,
            "has_required_columns": True,
            "no_missing_prices": df[['open', 'high', 'low', 'close']].isnull().sum().sum() == 0,
            "positive_prices": (df[['open', 'high', 'low', 'close']] > 0).all().all(),
            "reasonable_volume": df['volume'].mean() > 1000,
            "no_zero_volume_bars": (df['volume'] == 0).sum() / len(df) < 0.05,
            "no_duplicate_dates": df['date'].nunique() == len(df) if 'date' in df.columns else True,
            "high_low_consistency": (df['high'] >= df['low']).all(),
            "ohlc_consistency": (df['close'] >= df['low']).all() and (df['close'] <= df['high']).all()
        }
        
        return checks
    
    def validate_fundamentals(self, data: Dict) -> bool:
        """Validate fundamentals data."""
        try:
            required_keys = ['roe', 'roa', 'debt_to_equity', 'current_ratio']
            if not all(k in data for k in required_keys):
                return False
            
            # Check ratio bounds
            if not (0 <= data.get('roe', 0) <= 1):
                return False
            if not (0 <= data.get('roa', 0) <= 1):
                return False
            if not (0 <= data.get('debt_to_equity', 0) <= 10):
                return False
            if not (0 <= data.get('current_ratio', 0) <= 100):
                return False
            
            return True
        except:
            return False

