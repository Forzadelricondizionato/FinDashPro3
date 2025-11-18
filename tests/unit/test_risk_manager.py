import pytest
from fdp.trading.risk_manager import RiskManager
from fdp.trading.broker_adapter_enhanced import EnhancedOrder

class TestRiskManager:
    def test_validate_order_position_size_pass(self, mock_config):
        """Test order validation passes."""
        risk = RiskManager()
        
        order = EnhancedOrder(
            symbol="AAPL",
            action="buy",
            quantity=10,
            order_type="limit",
            limit_price=150.0
        )
        
        result = risk.validate_order(order)
        
        assert result["allowed"] is True
        assert result["reason"] == "OK"
    
    def test_validate_order_position_size_fail(self, mock_config):
        """Test order validation fails on large position."""
        risk = RiskManager()
        
        order = EnhancedOrder(
            symbol="AAPL",
            action="buy",
            quantity=1000,  # Large position
            order_type="limit",
            limit_price=150.0
        )
        
        result = risk.validate_order(order)
        
        assert result["allowed"] is False
        assert "exceeds limit" in result["reason"]
    
    def test_calculate_var_no_positions(self):
        """Test VaR with no positions."""
        risk = RiskManager()
        var = risk.calculate_var([])
        assert var == 0.0
    
    def test_calculate_var_with_positions(self):
        """Test VaR calculation."""
        risk = RiskManager()
        positions = [
            {"market_value": 50000},
            {"market_value": 30000}
        ]
        var = risk.calculate_var(positions)
        assert var > 0
    
    def test_correlation_risk_diversified(self):
        """Test correlation risk for diversified portfolio."""
        risk = RiskManager()
        positions = [
            {"symbol": "AAPL", "market_value": 10000},
            {"symbol": "GOOGL", "market_value": 10000},
            {"symbol": "MSFT", "market_value": 10000}
        ]
        
        risk_check = risk.check_correlation_risk(positions)
        assert risk_check["risk_level"] == "low"
    
    def test_correlation_risk_concentrated(self):
        """Test correlation risk for concentrated portfolio."""
        risk = RiskManager()
        positions = [
            {"symbol": "AAPL", "market_value": 10000}
        ]
        
        risk_check = risk.check_correlation_risk(positions)
        assert risk_check["risk_level"] == "high"
