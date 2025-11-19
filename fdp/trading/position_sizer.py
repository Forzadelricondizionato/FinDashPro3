import numpy as np
import structlog
from fdp.core.config import config

logger = structlog.get_logger()

class KellyPositionSizer:
    def __init__(self):
        self.min_position = 100.0
        self.max_position_pct = config.max_position_percent / 100.0
        self.kelly_fraction = config.kelly_fraction

    def calculate_position_size(self, win_probability: float, win_loss_ratio: float,
                                account_summary: dict, predicted_return: float) -> float:
        if win_loss_ratio <= 0 or win_probability <= 0 or win_probability >= 1:
            logger.warning("kelly_invalid_params", win_prob=win_probability, win_loss=win_loss_ratio)
            return 0.0
        volatility_30d = account_summary.get("volatility_30d", 0.02)
        if volatility_30d == 0:
            logger.warning("kelly_zero_volatility_fallback")
            volatility_30d = 0.02
        q = 1 - win_probability
        kelly_fraction_raw = (win_probability * win_loss_ratio - q) / win_loss_ratio
        kelly_fraction_adj = kelly_fraction_raw * self.kelly_fraction
        kelly_fraction_adj = min(kelly_fraction_adj, self.max_position_pct)
        if kelly_fraction_adj <= 0:
            return 0.0
        total_capital = account_summary.get("total_cash", 0)
        position_size = total_capital * kelly_fraction_adj
        if position_size < self.min_position:
            logger.warning("position_size_too_small", size=position_size, min=self.min_position)
            return 0.0
        logger.info("kelly_position_calculated", size=position_size, fraction=kelly_fraction_adj)
        return position_size
