import pytest
import asyncio
from unittest.mock import patch, AsyncMock
from fdp.core.orchestrator import FinDashProOrchestrator

pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.slow]

class TestOrchestratorIntegration:
    async def test_full_orchestrator_flow(self, redis_client, db_pool, mock_config, sample_ohlcv, sample_fundamentals):
        """Test complete orchestrator flow with mocks."""
        # Mock external dependencies
        with patch('fdp.data.providers.market_data.MultiSourceMarketDataManager') as mock_md, \
             patch('fdp.data.providers.fundamentals.FundamentalsManager') as mock_fund, \
             patch('fdp.data.providers.sentiment.AdvancedSentimentAnalyzer') as mock_sentiment, \
             patch('fdp.ml.stacking_ensemble.StackingEnsemble') as mock_model:
            
            # Setup mocks
            md_instance = AsyncMock()
            md_instance.__aenter__ = AsyncMock(return_value=md_instance)
            md_instance.fetch_ohlcv = AsyncMock(return_value=sample_ohlcv)
            mock_md.return_value = md_instance
            
            fund_instance = AsyncMock()
            fund_instance.__aenter__ = AsyncMock(return_value=fund_instance)
            fund_instance.get_latest = AsyncMock(return_value=sample_fundamentals)
            mock_fund.return_value = fund_instance
            
            sentiment_instance = AsyncMock()
            sentiment_instance.__aenter__ = AsyncMock(return_value=sentiment_instance)
            sentiment_instance.analyze_comprehensive = AsyncMock(return_value={"composite_score": 0.5, "news_sentiment": 0, "social_sentiment": 0, "volume": 10})
            mock_sentiment.return_value = sentiment_instance
            
            model_instance = AsyncMock()
            model_instance.train = lambda X, y: {"ensemble": {"f1": 0.75}}
            model_instance.predict = lambda X: {"direction": 1, "probabilities": [0.8], "expected_return": 0.05}
            mock_model.return_value = model_instance
            
            # Create orchestrator
            orchestrator = FinDashProOrchestrator()
            orchestrator.running = True  # Keep running for test
            
            # Load small universe
            with patch.object(orchestrator, 'load_ticker_universe', AsyncMock(return_value=pd.DataFrame([
                {"symbol": "AAPL", "region": "usa", "type": "stock"},
                {"symbol": "GOOGL", "region": "usa", "type": "stock"},
            ]))):
                
                # Mock process_ticker to avoid long processing
                with patch.object(orchestrator, 'process_ticker', AsyncMock(return_value={
                    "status": "success",
                    "ticker": "AAPL",
                    "confidence": 0.8
                })):
                    
                    await orchestrator.init_db_pool()
                    await orchestrator.init_redis_streams()
                    
                    # Run producer and consume
                    asyncio.create_task(orchestrator.producer(pd.DataFrame([
                        {"symbol": "AAPL", "region": "usa", "type": "stock"},
                    ])))
                    
                    # Give time to process
                    await asyncio.sleep(2)
                    
                    # Check results
                    signals = await redis_client.llen("signals:queue")
                    assert signals == 1
                    
                    await orchestrator.close_db_pool()
