"""Test configuration with async fixtures and mocks."""
import pytest
import pytest_asyncio
import asyncio
from unittest.mock import Mock, AsyncMock, patch
import redis.asyncio as redis
import asyncpg
import pandas as pd
import numpy as np
from pathlib import Path

from fdp.core.config import Config
from fdp.core.rate_limiter import TokenBucketRateLimiter
from fdp.core.circuit_breaker import CircuitBreaker
from fdp.data.providers.market_data import MultiSourceMarketDataManager
from fdp.data.providers.fundamentals import FundamentalsManager
from fdp.ml.features import FeatureEngineering

@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture
async def mock_redis():
    """Mock Redis with real commands."""
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    yield redis_client
    await redis_client.flushdb()  # Clean up after tests
    await redis_client.close()

@pytest_asyncio.fixture
async def mock_db_pool():
    """Mock PostgreSQL pool."""
    pool = AsyncMock()
    pool.acquire = AsyncMock()
    pool.acquire.return_value.__aenter__ = AsyncMock()
    pool.acquire.return_value.__aexit__ = AsyncMock()
    pool.execute = AsyncMock(return_value=None)
    pool.fetch = AsyncMock(return_value=[])
    pool.close = AsyncMock()
    yield pool

@pytest.fixture
def test_config():
    """Test configuration."""
    return Config(
        execution_mode="paper",
        max_tickers=10,
        min_confidence=0.75,
        daily_api_budget=5.0,
        redis_url="redis://localhost:6379",
        database_url="postgresql://test:test@localhost:5432/test",
        # Add all required fields
        ibkr_host="localhost",
        ibkr_port=4002,
        ibkr_client_id=123,
        ibkr_readonly=True,
        ibkr_trading_mode="paper",
        alpaca_key="test_key",
        alpaca_secret="test_secret",
        alpaca_paper=True,
        fmp_api_key="test_fmp",
        finnhub_api_key="test_finnhub",
        polygon_key="test_polygon",
        alpha_key="test_alpha",
        tiingo_key="test_tiingo",
        telegram_token=None,
        telegram_chat_id=None,
        discord_webhook=None,
        # Fundamentals validation
        min_current_ratio=1.0,
        max_debt_to_equity=2.0,
        min_gross_margin=0.10,
        min_operating_margin=0.05,
        min_net_margin=0.05,
        min_roe=0.08,
        min_roa=0.04,
        min_interest_coverage=3.0
    )

@pytest.fixture
def sample_ohlcv():
    """Sample OHLCV data."""
    dates = pd.date_range(start='2023-01-01', periods=365, freq='D')
    np.random.seed(42)
    close = 100 + np.random.randn(365).cumsum()
    
    return pd.DataFrame({
        'date': dates,
        'open': close - np.random.randn(365) * 0.5,
        'high': close + np.random.randn(365) * 0.5 + 1,
        'low': close - np.random.randn(365) * 0.5 - 1,
        'close': close,
        'volume': np.random.randint(1e6, 5e6, 365)
    })

@pytest.fixture
def sample_fundamentals():
    """Sample fundamentals data."""
    return {
        "roe": 0.15,
        "roa": 0.08,
        "debt_to_equity": 0.5,
        "current_ratio": 2.5,
        "net_margin": 0.12,
        "gross_margin": 0.35,
        "operating_margin": 0.18,
        "interest_coverage": 8.0,
        "pe_ratio": 18.0,
        "market_cap": 1e10,
        "source": "test"
    }

@pytest.fixture
def sample_sentiment():
    """Sample sentiment data."""
    return {
        "news_sentiment": 0.5,
        "social_sentiment": 0.3,
        "composite_score": 0.4,
        "volume": 100
    }

@pytest_asyncio.fixture
async def rate_limiter(mock_redis):
    """Test rate limiter."""
    return TokenBucketRateLimiter(mock_redis, budget=5.0)

@pytest_asyncio.fixture
async def circuit_breaker():
    """Test circuit breaker."""
    return CircuitBreaker(failure_threshold=3, recovery_timeout=60)

@pytest.fixture
def feature_engineer():
    """Test feature engineer."""
    return FeatureEngineering()

@pytest.fixture
def temp_model_dir(tmp_path):
    """Temporary model directory."""
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    return model_dir

# Helper functions
def create_test_signal(ticker="AAPL", action="buy", confidence=0.85):
    """Create test signal."""
    return {
        "ticker": ticker,
        "action": action,
        "confidence": confidence,
        "predicted_return": 0.05,
        "timestamp": "2024-01-01T12:00:00"
    }

def create_test_order():
    """Create test order."""
    return {
        "symbol": "AAPL",
        "action": "buy",
        "quantity": 100,
        "order_type": "limit",
        "limit_price": 150.0
    }
