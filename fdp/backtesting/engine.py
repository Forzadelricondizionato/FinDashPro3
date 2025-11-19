import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List
import redis.asyncio as redis
import structlog

logger = structlog.get_logger()

class BacktestEngine:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.initial_capital = config.paper_trading_capital
        self.commission = 0.001
        self.slippage = 0.0005

    async def run_backtest(self, ticker:
