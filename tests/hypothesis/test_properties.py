# tests/hypothesis/test_properties.py
import pytest
from hypothesis import given, strategies as st, settings
from fdp.trading.position_sizer import KellyPositionSizer
from fdp.trading.risk_manager import RiskManager
from fdp.trading.broker_adapter_enhanced import EnhancedOrder

class TestKellyProperties:
    @given(
        win_prob=st.floats(min_value=0.1, max_value=0.9),
        win_loss_ratio=st.floats(min_value=0.5, max_value=5.0),
        cash=st.floats(min_value=1000, max_value=1_000_000),
        portfolio_value=st.floats(min_value=1000, max_value=10_000_000)
    )
    @settings(max_examples=100)
    def test_kelly_position_bounds(self, win_prob, win_loss_ratio, cash, portfolio_value):
        sizer = KellyPositionSizer(kelly_fraction=0.25, max_position_size_fraction=0.25)
        position = sizer.calculate_position_size(
            win_probability=win_prob,
            win_loss_ratio=win_loss_ratio,
            account_summary={"cash": cash, "portfolio_value": portfolio_value}
        )
        assert 0 <= position <= min(cash, portfolio_value * 0.25)
        assert isinstance(position, (int, float))

class TestRiskManagerProperties:
    @given(
        quantity=st.integers(min_value=1, max_value=10000),
        limit_price=st.floats(min_value=0.01, max_value=10000),
        max_position=st.floats(min_value=1000, max_value=1000000)
    )
    def test_risk_bounds(self, quantity, limit_price, max_position):
        risk = RiskManager(max_position_size_usd=max_position)
        order = EnhancedOrder(symbol="TEST", action="buy", quantity=quantity, order_type="limit", limit_price=limit_price)
        result = risk.validate_order(order)
        assert isinstance(result["allowed"], bool)
        if quantity * limit_price > max_position:
            assert result["allowed"] is False
        else:
            assert result["allowed"] is True
