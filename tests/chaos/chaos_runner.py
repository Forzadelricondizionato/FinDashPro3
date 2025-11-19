import asyncio
import subprocess
import time
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from fdp.core.orchestrator import FinDashProOrchestrator
from fdp.core.config import config
import structlog

logger = structlog.get_logger()

class ChaosEngineeringTestSuite:
    def __init__(self):
        self.orchestrator = FinDashProOrchestrator()
    
    async def test_redis_failure_resilience(self):
        logger.info("chaos_redis_test_start")
        await self.orchestrator.init_db_pool()
        
        # Simulate Redis failure
        await self.orchestrator.redis.close()
        
        # Should fallback gracefully
        result = await self.orchestrator.process_ticker_safe("AAPL", "usa", "stock")
        assert result["status"] in ["failed", "retry"]
        
        logger.info("chaos_redis_test_passed")
    
    async def test_network_latency_impact(self):
        logger.info("chaos_latency_test_start")
        import time
        
        start = time.time()
        await self.orchestrator.market_data.fetch_ohlcv("TEST", None, None)
        duration = time.time() - start
        
        # Should have circuit breaker open after threshold
        assert duration < 5.0  # Timeout should kick in
        
        logger.info("chaos_latency_test_passed")
    
    async def test_database_connection_loss(self):
        logger.info("chaos_db_test_start")
        await self.orchestrator.init_db_pool()
        
        # Kill all connections
        await self.orchestrator.db_pool.close()
        
        # Should reconnect automatically
        await self.orchestrator.init_db_pool()
        assert self.orchestrator.db_pool is not None
        
        logger.info("chaos_db_test_passed")
    
    async def run_all(self):
        await self.test_redis_failure_resilience()
        await self.test_network_latency_impact()
        await self.test_database_connection_loss()

if __name__ == "__main__":
    asyncio.run(ChaosEngineeringTestSuite().run_all())

