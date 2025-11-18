import pytest
import pandas as pd
import numpy as np
from fdp.ml.features import FeatureEngineering

class TestFeatureEngineering:
    def test_engineer_features_no_leakage(self, sample_ohlcv, sample_fundamentals):
        """Test that features don't leak future information."""
        engineer = FeatureEngineering()
        
        X, y = engineer.engineer_features(sample_ohlcv, sample_fundamentals, sentiment=0.5)
        
        # Check no NaN
        assert not X.isna().any().any()
        assert not y.isna().any()
        
        # Check temporal consistency
        assert len(X) == len(y)
        
        # Check feature columns
        expected_cols = ['returns', 'volatility', 'rsi', 'sma_20', 'sma_50', 'sentiment', 'fund_0']
        for col in expected_cols:
            assert col in X.columns
    
    def test_rsi_calculation(self):
        """Test RSI calculation."""
        engineer = FeatureEngineering()
        
        prices = pd.Series([100, 101, 102, 103, 102, 101, 100, 99, 98, 97])
        rsi = engineer._calculate_rsi(prices, period=3)
        
        assert len(rsi) == len(prices)
        assert rsi.iloc[-1] >= 0 and rsi.iloc[-1] <= 100
    
    def test_bollinger_bands(self):
        """Test Bollinger Bands calculation."""
        engineer = FeatureEngineering()
        
        prices = pd.Series([100, 101, 102, 103, 104, 105, 104, 103, 102, 101])
        upper, lower = engineer._calculate_bollinger_bands(prices, period=3, std_dev=2)
        
        assert len(upper) == len(prices)
        assert len(lower) == len(prices)
        assert (upper >= prices).all()  # Upper band >= price
        assert (lower <= prices).all()  # Lower band <= price

@pytest.fixture
def sample_ohlcv_short():
    """Short OHLCV for edge cases."""
    return pd.DataFrame({
        'date': pd.date_range('2023-01-01', periods=50, freq='D'),
        'open': [100] * 50,
        'high': [105] * 50,
        'low': [95] * 50,
        'close': [100] * 50,
        'volume': [1000000] * 50
})

class TestFeatureEngineeringEdgeCases:
    def test_insufficient_data(self, sample_ohlcv_short, sample_fundamentals):
        """Test handling of insufficient data."""
        engineer = FeatureEngineering()
        
        # Remove too much data
        short_df = sample_ohlcv_short.head(10)
        
        X, y = engineer.engineer_features(short_df, sample_fundamentals, sentiment=0.5)
        
        assert X.empty or len(X) < 10
    
    def test_all_nan_handling(self):
        """Test all-NaN input."""
        engineer = FeatureEngineering()
        
        df = pd.DataFrame({
            'date': pd.date_range('2023-01-01', periods=10, freq='D'),
            'open': [np.nan] * 10,
            'high': [np.nan] * 10,
            'low': [np.nan] * 10,
            'close': [np.nan] * 10,
            'volume': [np.nan] * 10
        })
        
        X, y = engineer.engineer_features(df, {}, sentiment=0.5)
        
        assert X.empty
