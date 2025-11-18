import pytest
import asyncio
import pandas as pd
from fdp.backtesting.engine import BacktestEngine

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]

class TestBacktestEngine:
    async def test_backtest_basic_flow(self, redis_client):
        """Test complete backtest flow."""
        engine = BacktestEngine(redis_client)
        
        signals = [
            {
                "ticker": "AAPL",
                "action": "buy",
                "timestamp": "2023-01-15T10:00:00"
            },
            {
                "ticker": "AAPL",
                "action": "sell",
                "timestamp": "2023-01-20T10:00:00"
            }
        ]
        
        result = await engine.run_backtest("AAPL", signals, initial_capital=10000)
        
        assert "initial_capital" in result
        assert "final_value" in result
        assert "total_pnl" in result
        assert "returns_percent" in result
        assert len(result["trades"]) > 0
    
    async def test_backtest_no_signals(self, redis_client):
        """Test backtest with no signals."""
        engine = BacktestEngine(redis_client)
        
        result = await engine.run_backtest("AAPL", [], initial_capital=10000)
        
        assert result["initial_capital"] == 10000
        assert result["final_value"] == 10000
        assert result["total_pnl"] == 0
        assert result["trades"] == []
    
    async def test_backtest_insufficient_data(self, redis_client):
        """Test backtest with invalid ticker."""
        engine = BacktestEngine(redis_client)
        
        result = await engine.run_backtest("INVALID_TICKER_XXX", [])
        
        assert "error" in result
        assert "No historical data" in result["error"]
    
    async def test_get_backtest_results(self, redis_client):
        """Test retrieving backtest results."""
        engine = BacktestEngine(redis_client)
        
        # Run a test first
        signals = [
            {
                "ticker": "TEST",
                "action": "buy",
                "timestamp": "2023-01-15T10:00:00"
            }
        ]
        await engine.run_backtest("TEST", signals)
        
        # Get results
        results = await engine.get_backtest_results("TEST")
        
        assert results["total_backtests"] > 0
        assert len(results["results"]) > 0
