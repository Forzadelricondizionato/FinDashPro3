import pytest
import asyncio
import redis.asyncio as redis
import asyncpg
import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
import os
from unittest.mock import Mock
from fdp.core.config import Config

TEST_CONFIG = {
    "FDP_EXECUTION_MODE": "paper",
    "FDP_MAX_TICKERS": "10",
    "FDP_MIN_CONFIDENCE": "0.75",
    "FDP_REDIS_URL": "redis://localhost:6379/1",
    "FDP_DATABASE_URL": "postgresql://test:test@localhost:5432/findashpro_test",
    "FDP_DAILY_API_BUDGET": "5.0",
    "FDP_KILL_SWITCH_ENABLED": "0",
    "VAULT_TOKEN": "test-token",
}

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
async def redis_client():
    client = redis.from_url(TEST_CONFIG["FDP_REDIS_URL"], decode_responses=True)
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.close()

@pytest.fixture
async def db_pool():
    pool = await asyncpg.create_pool(TEST_CONFIG["FDP_DATABASE_URL"])
    yield pool
    await pool.close()

@pytest.fixture
def mock_config():
    return Config(
        execution_mode="paper",
        max_tickers=10,
        min_confidence=0.75,
        redis_url=TEST_CONFIG["FDP_REDIS_URL"],
        database_url=TEST_CONFIG["FDP_DATABASE_URL"],
        daily_api_budget=5.0,
        vault_token="test-token"
    )

@pytest.fixture
def sample_ohlcv():
    np.random.seed(42)
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
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)
