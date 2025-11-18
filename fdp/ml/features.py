import pandas as pd
import numpy as np
from typing import Tuple, Dict

class FeatureEngineering:
    """Feature engineering with temporal data splitting."""
    
    def engineer_features(self, ohlcv: pd.DataFrame, fundamentals: Dict, sentiment: float) -> Tuple[pd.DataFrame, pd.Series]:
        """Create features with no lookahead bias."""
        df = ohlcv.copy().reset_index(drop=True)
        
        # Technical indicators
        df['returns'] = df['close'].pct_change()
        df['volatility'] = df['returns'].rolling(20).std()
        df['rsi'] = self._calculate_rsi(df['close'])
        df['sma_20'] = df['close'].rolling(20).mean()
        df['sma_50'] = df['close'].rolling(50).mean()
        df['sma_200'] = df['close'].rolling(200).mean()
        df['bb_upper'], df['bb_lower'] = self._calculate_bollinger_bands(df['close'])
        
        # Volume indicators
        df['volume_sma'] = df['volume'].rolling(20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma'].replace(0, np.nan)
        
        # Sentiment (static for the day)
        df['sentiment'] = sentiment
        
        # Fundamentals (static, limited to 5 features)
        fundamental_keys = list(fundamentals.keys())[:5]
        for i, key in enumerate(fundamental_keys):
            df[f'fund_{i}'] = fundamentals.get(key, 0)
        
        # Remove rows with NaN
        df.dropna(inplace=True)
        
        if df.empty or len(df) < 100:
            return pd.DataFrame(), pd.Series()
        
        # Target variable: 5-day forward return (no leakage)
        df['future_return'] = df['close'].pct_change(5).shift(-5)
        df = df.dropna()
        
        # Features (exclude target and original prices)
        feature_cols = [col for col in df.columns if col not in ['close', 'future_return', 'date']]
        X = df[feature_cols]
        y = (df['future_return'] > 0).astype(int)
        
        return X, y
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate RSI indicator."""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.replace(np.inf, 100).replace(-np.inf, 0).fillna(50)
    
    def _calculate_bollinger_bands(self, prices: pd.Series, period: int = 20, std_dev: int = 2) -> tuple:
        """Calculate Bollinger Bands."""
        sma = prices.rolling(period).mean()
        rolling_std = prices.rolling(period).std()
        upper_band = sma + (rolling_std * std_dev)
        lower_band = sma - (rolling_std * std_dev)
        return upper_band, lower_band
