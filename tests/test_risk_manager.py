import pytest
from fdp.trading.risk_manager import RiskManager
from fdp.trading.broker_adapter_enhanced import EnhancedOrder

class TestRiskManager:
    def test_validate_order_pass(self):
        risk = RiskManager()
        order = EnhancedOrder(symbol="AAPL", action="buy", quantity=10, order_type="limit", limit_price=150.0)
        result = risk.validate_order(order)
        assert result["allowed"] is True
        assert result["reason"] == "OK"
    
    def test_validate_order_fail_position_size(self):
        risk = RiskManager(max_position_size_usd=1000)
        order = EnhancedOrder(symbol="AAPL", action="buy", quantity=100, order_type="limit", limit_price=150.0)
        result = risk.validate_order(order)
        assert result["allowed"] is False
        assert "exceeds limit" in result["reason"]
    
    def test_calculate_var_no_positions(self):
        risk = RiskManager()
        var = risk.calculate_var([])
        assert var == 0.0
    
    def test_calculate_var_with_positions(self):
        risk = RiskManager()
        positions = [
            {"market_value": 50000},
            {"market_value": 30000}
        ]
        var = risk.calculate_var(positions)
        assert var >= 0
    
    def test_correlation_risk_diversified(self):
        risk = RiskManager()
        positions = [
            {"symbol": "AAPL", "market_value": 10000},
            {"symbol": "GOOGL", "market_value": 10000},
            {"symbol": "MSFT", "market_value": 10000}
        ]
        risk_check = risk.check_correlation_risk(positions)
        assert risk_check["risk_level"] == "low"
    
    def test_correlation_risk_concentrated(self):
        risk = RiskManager()
        positions = [
            {"symbol": "AAPL", "market_value": 10000}
        ]
        risk_check = risk.check_correlation_risk(positions)
        assert risk_check["risk_level"] == "high"

