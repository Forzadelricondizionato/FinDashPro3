import pytest
from fdp.trading.position_sizer import KellyPositionSizer

class TestKellyPositionSizer:
    def test_kelly_calculation_basic(self):
        """Test basic Kelly calculation."""
        sizer = KellyPositionSizer(kelly_fraction=1.0)  # Full Kelly
        
        position = sizer.calculate_position_size(
            win_probability=0.6,
            win_loss_ratio=2.0,
            account_summary={"cash": 100000, "portfolio_value": 100000},
            edge=0.05
        )
        
        # Kelly: 0.6 - (1-0.6)/2 = 0.4
        # Full Kelly should be 40% of portfolio
        assert position == 40000
    
    def test_kelly_fractional(self):
        """Test fractional Kelly."""
        sizer = KellyPositionSizer(kelly_fraction=0.25)
        
        position = sizer.calculate_position_size(
            win_probability=0.6,
            win_loss_ratio=2.0,
            account_summary={"cash": 100000, "portfolio_value": 100000}
        )
        
        # Kelly: 0.4, Fractional: 0.1
        assert position == 10000
    
    def test_kelly_clamping(self):
        """Test position size clamping."""
        sizer = KellyPositionSizer(kelly_fraction=1.0)
        
        # High win rate should not exceed 25% clamp
        position = sizer.calculate_position_size(
            win_probability=0.95,
            win_loss_ratio=10.0,
            account_summary={"cash": 100000, "portfolio_value": 100000}
        )
        
        assert position == 25000  # 25% clamp
    
    def test_insufficient_cash(self):
        """Test cannot exceed cash."""
        sizer = KellyPositionSizer(kelly_fraction=1.0)
        
        position = sizer.calculate_position_size(
            win_probability=0.6,
            win_loss_ratio=2.0,
            account_summary={"cash": 10000, "portfolio_value": 100000}
        )
        
        assert position == 10000  # Limited by cash
    
    def test_invalid_inputs(self):
        """Test invalid probability/ratio returns 0."""
        sizer = KellyPositionSizer()
        
        # Invalid probability
        position = sizer.calculate_position_size(
            win_probability=1.5,  # > 1
            win_loss_ratio=2.0,
            account_summary={"cash": 100000, "portfolio_value": 100000}
        )
        assert position == 0.0
        
        # Invalid ratio
        position = sizer.calculate_position_size(
            win_probability=0.6,
            win_loss_ratio=0.0,  # <= 0
            account_summary={"cash": 100000, "portfolio_value": 100000}
        )
        assert position == 0.0
    
    def test_minimum_position_size(self):
        """Test minimum position size filter."""
        sizer = KellyPositionSizer()
        
        position = sizer.calculate_position_size(
            win_probability=0.51,  # Small edge
            win_loss_ratio=1.1,
            account_summary={"cash": 100000, "portfolio_value": 100000}
        )
        
        # Should be below $100 minimum
        assert position == 0.0
