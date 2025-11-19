import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from fdp.trading.broker_adapter_enhanced import (
    EnhancedOrder,
    PaperBrokerAdapter,
    IBKRBrokerAdapter,
    get_broker_adapter
)
from fdp.core.config import Config

pytestmark = pytest.mark.asyncio

@pytest.fixture
async def mock_config():
    config = Config(
        execution_mode="paper",
        redis_url="redis://localhost:6379/1",
        database_url="postgresql://test:test@localhost:5432/findashpro_test",
        daily_api_budget=5.0,
        paper_trading_capital=100000
    )
    return config

@pytest.fixture
async def mock_redis():
    import redis.asyncio as redis
    client = redis.from_url("redis://localhost:6379/1", decode_responses=True)
    yield client
    await client.flushdb()
    await client.close()

@pytest.fixture
async def mock_notifier():
    notifier = AsyncMock()
    notifier.send_alert = AsyncMock()
    return notifier

class TestPaperBrokerAdapter:
    async def test_place_order_buy(self, mock_config, mock_redis, mock_notifier):
        adapter = PaperBrokerAdapter(mock_config, mock_notifier, mock_redis)
        order = EnhancedOrder(symbol="AAPL", action="buy", quantity=10, order_type="limit", limit_price=150.0)
        order_id = await adapter.place_order(order)
        assert order_id.startswith("paper_AAPL_")
        assert adapter.positions["AAPL"]["quantity"] == 10
        summary = await adapter.get_account_summary()
        assert summary["cash"] == 98500
    
    async def test_place_order_sell(self, mock_config, mock_redis, mock_notifier):
        adapter = PaperBrokerAdapter(mock_config, mock_notifier, mock_redis)
        buy_order = EnhancedOrder(symbol="AAPL", action="buy", quantity=10, order_type="limit", limit_price=150.0)
        await adapter.place_order(buy_order)
        sell_order = EnhancedOrder(symbol="AAPL", action="sell", quantity=10, order_type="limit", limit_price=160.0)
        order_id = await adapter.place_order(sell_order)
        assert "AAPL" not in adapter.positions
        summary = await adapter.get_account_summary()
        assert summary["cash"] == 100100
    
    async def test_duplicate_order_suppression(self, mock_config, mock_redis, mock_notifier):
        adapter = PaperBrokerAdapter(mock_config, mock_notifier, mock_redis)
        order = EnhancedOrder(symbol="AAPL", action="buy", quantity=10, order_type="limit", limit_price=150.0, idempotency_key="dup123")
        order_id1 = await adapter.place_order(order)
        order_id2 = await adapter.place_order(order)
        assert order_id2 == "duplicate_suppressed"

class TestIBKRBrokerAdapter:
    async def test_ibkr_connection(self, mock_config):
        with patch("ib_insync.IB") as mock_ib:
            adapter = IBKRBrokerAdapter(mock_config)
            await adapter.connect()
            mock_ib.return_value.connect.assert_called_once()

class TestBrokerFactory:
    def test_get_broker_adapter_paper(self, mock_config):
        adapter = get_broker_adapter(mock_config, None, None)
        assert isinstance(adapter, PaperBrokerAdapter)
    
    def test_get_broker_adapter_ibkr(self, mock_config):
        mock_config.execution_mode = "ibkr"
        adapter = get_broker_adapter(mock_config, None, None)
        assert isinstance(adapter, IBKRBrokerAdapter)

