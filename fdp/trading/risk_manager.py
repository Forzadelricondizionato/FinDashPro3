from typing import List, Dict, Any
import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

class RiskManager(BaseModel):
    max_position_size_usd: float = Field(default=25000)
    max_position_fraction: float = Field(default=0.25, le=1.0)
    correlation_threshold: float = Field(default=0.8)
    
    def validate_order(self, order: Any) -> Dict[str, Any]:
        if order.quantity * (order.limit_price or 0) > self.max_position_size_usd:
            return {"allowed": False, "reason": f"Position exceeds {self.max_position_size_usd} USD limit"}
        return {"allowed": True, "reason": "OK"}
    
    def calculate_var(self, positions: List[Dict[str, float]], confidence: float = 0.95) -> float:
        if not positions:
            return 0.0
        values = [pos.get("market_value", 0) for pos in positions]
        return np.percentile(values, (1 - confidence) * 100)
    
    def check_correlation_risk(self, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
        if len(positions) < 2:
            return {"risk_level": "low", "message": "Insufficient positions for correlation check"}
        
        df = pd.DataFrame({
            "symbol": [p["symbol"] for p in positions],
            "market_value": [p["market_value"] for p in positions]
        })
        
        high_correlation = any(df["market_value"].corr() > self.correlation_threshold)
        if high_correlation:
            return {"risk_level": "high", "message": "Portfolio concentration risk detected"}
        return {"risk_level": "low", "message": "Portfolio well-diversified"}
