import pytest
from unittest.mock import AsyncMock, patch
from fdp.trading.broker_adapter_enhanced import (
    PaperBrokerAdapter, 
    AlpacaBrokerAdapter, 
    EnhancedOrder,
    get_broker_adapter
)
import asyncpg

pytestmark = pytest.mark.asyncio

class TestPaperBrokerAdapter:
    async def test_paper_broker_place_order(self, mock_config, redis_client):
        """Test paper broker order placement."""
        adapter = PaperBrokerAdapter(mock_config, None, redis_client)
        await adapter.init_db()
        
        order = EnhancedOrder(
            symbol="AAPL",
            action="buy",
            quantity=10,
            order_type="limit",
            limit_price=150.0,
            idempotency_key="test_123"
        )
        
        order_id = await adapter.place_order(order)
        
        assert order_id.startswith("paper_AAPL_")
        assert order.status == "filled"
        
        # Check position created
        positions = await adapter.get_positions()
        assert "AAPL" in positions
        assert positions["AAPL"]["quantity"] == 10
        
        # Check cash deducted
        summary = await adapter.get_account_summary()
        assert summary["cash"] < summary["initial_capital"]
        
        await adapter.graceful_shutdown()
    
    async def test_paper_broker_duplicate_suppression(self, mock_config, redis_client):
        """Test idempotency key prevents duplicates."""
        adapter = PaperBrokerAdapter(mock_config, None, redis_client)
        await adapter.init_db()
        
        order = EnhancedOrder(
            symbol="AAPL",
            action="buy",
            quantity=10,
            order_type="limit",
            limit_price=150.0,
            idempotency_key="test_dup"
        )
        
        # First order
        order_id1 = await adapter.place_order(order)
        
        # Second identical order
        order_id2 = await adapter.place_order(order)
        
        assert order_id2 == "duplicate_suppressed"
        
        await adapter.graceful_shutdown()
    
    async def test_paper_broker_sell_position(self, mock_config, redis_client):
        """Test selling existing position."""
        adapter = PaperBrokerAdapter(mock_config, None, redis_client)
        await adapter.init_db()
        
        # Buy first
        buy_order = EnhancedOrder(
            symbol="AAPL",
            action="buy",
            quantity=10,
            order_type="limit",
            limit_price=150.0
        )
        await adapter.place_order(buy_order)
        
        # Then sell
        sell_order = EnhancedOrder(
            symbol="AAPL",
            action="sell",
            quantity=10,
            order_type="limit",
            limit_price=160.0
        )
        await adapter.place_order(sell_order)
        
        # Position should be closed
        positions = await adapter.get_positions()
        assert "AAPL" not in positions
        
        # Cash should increase
        summary = await adapter.get_account_summary()
        assert summary["cash"] > summary["initial_capital"]  # Profit from sell
        
        await adapter.graceful_shutdown()

class TestAlpacaBrokerAdapter:
    async def test_alpaca_get_account_summary(self, mock_config):
        """Test Alpaca account summary fetch."""
        adapter = AlpacaBrokerAdapter(mock_config, None, None)
        
        # Mock successful response
        mock_response = {
            "cash": "50000.00",
            "portfolio_value": "75000.00"
        }
        
        with patch.object(adapter, 'session') as mock_session:
            mock_get = AsyncMock()
            mock_get.return_value.status = 200
            mock_get.return_value.json = AsyncMock(return_value=mock_response)
            mock_session.get = mock_get
            
            # Need to enter context manager
            async with adapter:
                summary = await adapter.get_account_summary()
                
                assert summary["cash"] == 50000.0
                assert summary["portfolio_value"] == 75000.0
    
    async def test_alpaca_place_order_success(self, mock_config):
        """Test Alpaca order placement success."""
        adapter = AlpacaBrokerAdapter(mock_config, None, None)
        order = EnhancedOrder(
            symbol="AAPL",
            action="buy",
            quantity=10,
            order_type="limit",
            limit_price=150.0,
            idempotency_key="test_123"
        )
        
        mock_response = {"id": "order_12345"}
        
        with patch.object(adapter, 'session') as mock_session:
            mock_post = AsyncMock()
            mock_post.return_value.status = 200
            mock_post.return_value.json = AsyncMock(return_value=mock_response)
            mock_session.post = mock_post
            
            async with adapter:
                order_id = await adapter.place_order(order)
                assert order_id == "order_12345"

def test_broker_factory():
    """Test broker adapter factory."""
    with patch('fdp.core.config.config') as mock_config:
        mock_config.execution_mode = "paper"
        mock_config.paper_trading_capital = 100000
        mock_config.database_url = "postgresql://test:test@localhost/test"
        
        adapter = get_broker_adapter(mock_config, None, None)
        
        assert isinstance(adapter, PaperBrokerAdapter)
