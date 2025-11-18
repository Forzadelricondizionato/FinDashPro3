import pytest
import asyncio
import redis.asyncio as redis
import asyncpg
from pathlib import Path
import tempfile
import os
import json
from unittest.mock import Mock, AsyncMock, patch
from fdp.core.config import Config

# Test configuration
TEST_CONFIG = {
    "FDP_EXECUTION_MODE": "alert_only",
    "FDP_MAX_TICKERS": "10",
    "FDP_MIN_CONFIDENCE": "0.75",
    "FDP_REDIS_URL": "redis://localhost:6379/1",  # Use DB 1 for tests
    "FDP_DATABASE_URL": "postgresql://test:test@localhost:5432/findashpro_test",
    "FDP_DAILY_API_BUDGET": "5.0",
    "FDP_KILL_SWITCH_ENABLED": "0",  # Disable for tests
    "FDP_RL_YAHOO": "2000",
    "FDP_RL_ALPHA": "500",
    "ALPHA_VANTAGE_API_KEY": "demo",
    "TIINGO_API_KEY": "demo",
}

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
async def redis_client():
    """Create test Redis client."""
    client = redis.from_url(TEST_CONFIG["FDP_REDIS_URL"], decode_responses=True)
    yield client
    await client.flushdb()  # Clean up after test
    await client.close()

@pytest.fixture
async def db_pool():
    """Create test database pool."""
    pool = await asyncpg.create_pool(TEST_CONFIG["FDP_DATABASE_URL"])
    yield pool
    await pool.close()

@pytest.fixture
def mock_config(monkeypatch):
    """Mock configuration."""
    for key, value in TEST_CONFIG.items():
        monkeypatch.setenv(key, value)
    
    # Reload config
    import fdp.core.config
    fdp.core.config.config = fdp.core.config.Config()
    fdp.core.config.config.validate()
    
    return fdp.core.config.config

@pytest.fixture
def sample_ohlcv():
    """Sample OHLCV data."""
    return pd.DataFrame({
        'date': pd.date_range('2023-01-01', periods=200, freq='D'),
        'open': np.random.randn(200).cumsum() + 100,
        'high': np.random.randn(200).cumsum() + 105,
        'low': np.random.randn(200).cumsum() + 95,
        'close': np.random.randn(200).cumsum() + 100,
        'volume': np.random.randint(1e6, 5e6, 200)
    })

@pytest.fixture
def sample_fundamentals():
    """Sample fundamentals data."""
    return {
        "roe": 0.15,
        "roa": 0.08,
        "debt_to_equity": 0.5,
        "current_ratio": 2.5,
        "net_margin": 0.20,
        "gross_margin": 0.40,
        "operating_margin": 0.15,
        "interest_coverage": 8.0,
        "pe_ratio": 18.0,
        "market_cap": 1e10,
        "source": "test"
    }

@pytest.fixture
def mock_broker_adapter():
    """Mock broker adapter."""
    with patch('fdp.trading.broker_adapter_enhanced.get_broker_adapter') as mock:
        broker = AsyncMock()
        broker.get_account_summary = AsyncMock(return_value={"cash": 100000, "portfolio_value": 100000})
        broker.place_order = AsyncMock(return_value="order_123")
        broker.sync_orders = AsyncMock()
        broker.graceful_shutdown = AsyncMock()
        mock.return_value = broker
        yield broker

@pytest.fixture
def mock_notifier():
    """Mock notifier."""
    with patch('fdp.notifications.manager.MultiChannelNotifier') as mock:
        notifier = AsyncMock()
        notifier.send_alert = AsyncMock()
        mock.return_value = notifier
        yield notifier

@pytest.fixture
def temp_dir():
    """Temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)

@pytest.fixture(autouse=True)
def setup_test_env(mock_config, temp_dir):
    """Setup test environment."""
    # Create data directories
    (temp_dir / "data").mkdir(exist_ok=True)
    (temp_dir / "data/models").mkdir(exist_ok=True)
    (temp_dir / "data/logs").mkdir(exist_ok=True)
    
    # Set working directory
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.chdir(temp_dir)
    yield
    monkeypatch.undo()

# pytest-asyncio configuration
pytest_plugins = ('pytest_asyncio',)
