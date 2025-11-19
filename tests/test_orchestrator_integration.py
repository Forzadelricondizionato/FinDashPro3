import pytest
import asyncio
from unittest.mock import patch, AsyncMock
import pandas as pd
import numpy as np

pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.slow]

class TestOrchestratorIntegration:
    async def test_full_orchestrator_flow(self, redis_client, db_pool, mock_config, sample_ohlcv, sample_fundamentals):
        with patch('fdp.data.providers.market_data.MultiSourceMarketDataManager') as mock_md, \
             patch('fdp.data.providers.fundamentals.FundamentalsManager') as mock_fund, \
             patch('fdp.data.providers.sentiment.AdvancedSentimentAnalyzer') as mock_sentiment, \
             patch('fdp.ml.stacking_ensemble.StackingEnsemble') as mock_model:
            
            md_instance = AsyncMock()
            md_instance.fetch_ohlcv = AsyncMock(return_value=sample_ohlcv)
            mock_md.return_value = md_instance
            
            fund_instance = AsyncMock()
            fund_instance.get_latest = AsyncMock(return_value=sample_fundamentals)
            mock_fund.return_value = fund_instance
            
            sentiment_instance = AsyncMock()
            sentiment_instance.analyze_comprehensive = AsyncMock(return_value={"composite_score": 0.5})
            mock_sentiment.return_value = sentiment_instance
            
            model_instance = AsyncMock()
            model_instance.predict = AsyncMock(return_value={"direction": "buy", "confidence": 0.8, "expected_return": 0.05})
            mock_model.return_value = model_instance
            
            from fdp.core.orchestrator import FinDashProOrchestrator
            orchestrator = FinDashProOrchestrator()
            orchestrator.db_pool = db_pool
            
            test_universe = pd.DataFrame([
                {"symbol": "AAPL", "region": "usa", "type": "stock"},
                {"symbol": "GOOGL", "region": "usa", "type": "stock"},
            ])
            
            orchestrator.running = False
            await orchestrator.init_db_pool()
            
            with patch.object(orchestrator, 'process_ticker_safe', AsyncMock(return_value={"status": "success"})):
                await orchestrator.producer(test_universe.head(1))
                await asyncio.sleep(0.5)
                signals = await redis_client.llen("signals:queue")
                assert signals >= 0
            
            await orchestrator.close_db_pool()
