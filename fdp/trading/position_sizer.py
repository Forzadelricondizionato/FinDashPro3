import numpy as np
from typing import Dict

class KellyPositionSizer:
    """Correct Kelly Criterion position sizing."""
    
    def __init__(self, kelly_fraction: float = 0.25):
        self.kelly_fraction = kelly_fraction
    
    def calculate_position_size(self, win_probability: float, win_loss_ratio: float,
                               account_summary: Dict, edge: float = 0.0) -> float:
        """
        Calculate position size using Kelly Criterion.
        
        Args:
            win_probability: Probability of winning (0-1)
            win_loss_ratio: Average win / average loss
            account_summary: Dict with 'cash', 'portfolio_value'
            edge: Expected return (not used in classic Kelly)
        """
        # Validate inputs
        if not (0 < win_probability < 1):
            logger.warning("invalid_win_probability", value=win_probability)
            return 0.0
        
        if win_loss_ratio <= 0:
            logger.warning("invalid_win_loss_ratio", value=win_loss_ratio)
            return 0.0
        
        # Classic Kelly formula: f* = p - (1-p)/b
        # where p = win_probability, b = win_loss_ratio
        kelly_fraction_raw = win_probability - (1 - win_probability) / win_loss_ratio
        
        # Apply fractional Kelly for risk management
        kelly_fraction_adj = kelly_fraction_raw * self.kelly_fraction
        
        # Clamp to prevent overexposure
        kelly_fraction_adj = np.clip(kelly_fraction_adj, -0.25, 0.25)
        
        # Calculate position size based on available cash
        cash = account_summary.get('cash', 0)
        portfolio_value = account_summary.get('portfolio_value', cash)
        
        if cash <= 0:
            return 0.0
        
        # Position size in USD
        position_size_usd = kelly_fraction_adj * portfolio_value
        
        # Don't exceed available cash
        position_size_usd = min(position_size_usd, cash)
        
        # Minimum position size (e.g., $100)
        if position_size_usd < 100:
            return 0.0
        
        return float(position_size_usd)
