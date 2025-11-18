"""Test market data providers."""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, AsyncMock

@pytest.mark.asyncio
async def test_ticker_validation():
    """Test ticker sanitization."""
    from fdp.data.providers.market_data import MultiSourceMarketDataManager
    
    manager = MultiSourceMarketDataManager(Mock(), Mock())
    
    # Valid tickers
    assert manager._validate_ticker("AAPL") == "AAPL"
    assert manager._validate_ticker("BTC-USD") == "BTC-USD"
    
    # Invalid tickers
    with pytest.raises(ValueError):
        manager._validate_ticker("../../etc/passwd")
    
    with pytest.raises
