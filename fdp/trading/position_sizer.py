import math
from typing import Dict, Any
from pydantic import BaseModel, Field, validator

class KellyPositionSizer(BaseModel):
    kelly_fraction: float = Field(default=0.25, ge=0.0, le=1.0)
    max_position_size_fraction: float = Field(default=0.25, ge=0.0, le=1.0)
    min_position_size: float = Field(default=100.0, ge=0.0)
    
    @validator("kelly_fraction")
    def validate_kelly(cls, v):
        if v > 0.5:
            import warnings
            warnings.warn("Full Kelly (>0.5) is not recommended for production")
        return v
    
    def calculate_position_size(self, win_probability: float, win_loss_ratio: float, 
                               account_summary: Dict[str, Any], edge: float = 0.0) -> float:
        if win_probability <= 0 or win_probability >= 1:
            return 0.0
        if win_loss_ratio <= 0:
            return 0.0
        
        kelly = win_probability - ((1 - win_probability) / win_loss_ratio)
        kelly = max(0.0, min(kelly, self.max_position_size_fraction))
        kelly_fractional = kelly * self.kelly_fraction
        
        portfolio_value = account_summary.get("portfolio_value", 0)
        position_size = portfolio_value * kelly_fractional
        
        cash = account_summary.get("cash", 0)
        position_size = min(position_size, cash)
        
        if position_size < self.min_position_size:
            return 0.0
        
        return round(position_size, 2)
