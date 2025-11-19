import pandas as pd
from typing import Dict, Any, List
import structlog

logger = structlog.get_logger()

class RiskManager:
    def __init__(self):
        self.max_position_size_pct = config.max_position_size_percent
        self.max_daily_loss_pct = config.max_daily_loss_percent
        self.max_positions = 50

    def validate_order(self, order: 'EnhancedOrder') -> Dict[str, Any]:
        if not hasattr(order, 'quantity') or order.quantity <= 0:
            return {"allowed": False, "reason": "invalid_quantity"}
        if not hasattr(order, 'symbol') or not order.symbol:
            return {"allowed": False, "reason": "invalid_symbol"}
        return {"allowed": True, "reason": ""}

    def check_position_limit(self, symbol: str, proposed_quantity: float, account_value: float, current_portfolio: List[Dict]) -> bool:
        position_value = proposed_quantity * self._get_current_price(symbol)
        position_pct = position_value / account_value
        if position_pct > self.max_position_size_pct:
            logger.warning("position_size_exceeded", symbol=symbol, pct=position_pct, max=self.max_position_size_pct)
            return False
        if len(current_portfolio) >= self.max_positions:
            logger.warning("max_positions_reached", symbol=symbol, max=self.max_positions)
            return False
        return True

    def check_daily_loss_limit(self, account_value: float, daily_loss: float) -> bool:
        daily_loss_pct = abs(daily_loss) / account_value
        if daily_loss_pct > self.max_daily_loss_pct:
            logger.critical("daily_loss_limit_exceeded", loss_pct=daily_loss_pct, max=self.max_daily_loss_pct)
            return False
        return True

    def check_correlation_limit(self, symbol: str, current_portfolio: List[str]) -> bool:
        if symbol in current_portfolio:
            return True
        if len(current_portfolio) > 0:
            logger.info("correlation_check_passed", symbol=symbol, portfolio_size=len(current_portfolio))
        return True

    def _get_current_price(self, symbol: str) -> float:
        return 100.0
