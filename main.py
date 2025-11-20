# main.py
import asyncio
import sys
from pathlib import Path
import redis.asyncio as redis
from fdp.core.orchestrator import FinDashProOrchestrator
from fdp.core.config import config
from fdp.core.rate_limiter import TokenBucketRateLimiter
from fdp.core.circuit_breaker import CircuitBreaker
from fdp.ml.stacking_ensemble import StackingEnsemble
from fdp.ml.features import FeatureEngineering
from fdp.trading.position_sizer import KellyPositionSizer
from fdp.trading.risk_manager import RiskManager
from fdp.trading.broker_adapter_enhanced import get_broker_adapter
from fdp.notifications.manager import MultiChannelNotifier
import structlog

logger = structlog.get_logger()

async def main():
    orchestrator = FinDashProOrchestrator()
    orchestrator.config = config
    
    orchestrator.redis = redis.from_url(config.redis_url, decode_responses=True)
    orchestrator.rate_limiter = TokenBucketRateLimiter(orchestrator.redis, config.daily_api_budget)
    orchestrator.circuit_breaker = CircuitBreaker()
    orchestrator.feature_engineer = FeatureEngineering()
    orchestrator.model = StackingEnsemble(Path("data/models"))
    orchestrator.position_sizer = KellyPositionSizer()
    orchestrator.risk_manager = RiskManager()
    orchestrator.notifier = MultiChannelNotifier(config, orchestrator.rate_limiter, Path("notifications/templates"))
    orchestrator.broker = get_broker_adapter(config, orchestrator.notifier, orchestrator.redis)
    
    try:
        await orchestrator.init_db_pool()
        await orchestrator.init_redis_streams()
        await orchestrator.broker.connect()
        
        universe = await orchestrator.load_ticker_universe()
        
        await asyncio.gather(
            orchestrator.producer(universe),
            orchestrator.consumer(),
            orchestrator.start_api_server()
        )
    finally:
        await orchestrator.close_db_pool()
        await orchestrator.broker.graceful_shutdown()

if __name__ == "__main__":
    asyncio.run(main())
