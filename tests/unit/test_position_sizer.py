# tests/unit/test_position_sizer.py
import pytest
from fdp.trading.position_sizer import KellyPositionSizer

class TestKellyPositionSizer:
    def test_kelly_calculation_basic(self):
        sizer = KellyPositionSizer(kelly_fraction=1.0, max_position_size_fraction=0.4)
        position = sizer.calculate_position_size(
            win_probability=0.6,
            win_loss_ratio=2.0,
            account_summary={"cash": 100000, "portfolio_value": 100000}
        )
        assert position == 40000
    
    def test_kelly_fractional(self):
        sizer = KellyPositionSizer(kelly_fraction=0.25)
        position = sizer.calculate_position_size(
            win_probability=0.6,
            win_loss_ratio=2.0,
            account_summary={"cash": 100000, "portfolio_value": 100000}
        )
        assert position == 10000
    
    def test_kelly_clamping(self):
        sizer = KellyPositionSizer(kelly_fraction=1.0, max_position_size_fraction=0.25)
        position = sizer.calculate_position_size(
            win_probability=0.95,
            win_loss_ratio=10.0,
            account_summary={"cash": 100000, "portfolio_value": 100000}
        )
        assert position == 25000
    
    def test_insufficient_cash(self):
        sizer = KellyPositionSizer(kelly_fraction=1.0)
        position = sizer.calculate_position_size(
            win_probability=0.6,
            win_loss_ratio=2.0,
            account_summary={"cash": 10000, "portfolio_value": 100000}
        )
        assert position == 10000
    
    def test_invalid_inputs(self):
        sizer = KellyPositionSizer()
        position = sizer.calculate_position_size(
            win_probability=1.5,
            win_loss_ratio=2.0,
            account_summary={"cash": 100000, "portfolio_value": 100000}
        )
        assert position == 0.0
        
        position = sizer.calculate_position_size(
            win_probability=0.6,
            win_loss_ratio=0.0,
            account_summary={"cash": 100000, "portfolio_value": 100000}
        )
        assert position == 0.0
    
    def test_minimum_position_size(self):
        sizer = KellyPositionSizer(min_position_size=100.0)
        position = sizer.calculate_position_size(
            win_probability=0.51,
            win_loss_ratio=1.1,
            account_summary={"cash": 100000, "portfolio_value": 100000}
        )
        assert position == 0.0
