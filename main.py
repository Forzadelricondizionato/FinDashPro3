#!/usr/bin/env python3
"""FinDashPro 3.1.1 - Orchestrator con Producer-Consumer Redis Streams & ML Monitoring"""
import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta
import signal
import json
from typing import List, Dict, Any, Optional
import traceback
import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).parent))

from fdp.core.config import config, ConfigValidationError
from fdp.core.rate_limiter import TokenBucketRateLimiter
from fdp.core.circuit_breaker import CircuitBreaker
from fdp.data.providers.market_data import MultiSourceMarketDataManager
from fdp.data.providers.fundamentals import FundamentalsManager
from fdp.data.providers.sentiment import AdvancedSentimentAnalyzer
from fdp.data.quality.gatekeeper import DataQualityGatekeeper
from fdp.ml.features import FeatureEngineering
from fdp.ml.feature_selector import AdvancedFeatureSelector
from fdp.ml.stacking_ensemble import StackingEnsemble
from fdp.ml.ops.model_flow import ModelRegistry
from fdp.ml.ops.drift_monitor import DriftMonitor
from fdp.ml.ops.ml_monitor import MLMonitoring
from fdp.trading.broker_adapter_enhanced import get_broker_adapter, EnhancedOrder
from fdp.trading.risk_manager import RiskManager
from fdp.trading.position_sizer import KellyPositionSizer
from fdp.notifications.manager import MultiChannelNotifier
from fdp.backtesting.engine import BacktestEngine
import redis.asyncio as redis
import asyncpg

import structlog

logger = structlog.get_logger(__name__)

class FinDashProOrchestrator:
    def __init__(self):
        self.redis = redis.from_url(config.redis_url, decode_responses=True)
        self.rate_limiter = TokenBucketRateLimiter(self.redis, config.daily_api_budget)
        self.circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=300)
        self.market_data = MultiSourceMarketDataManager(self.rate_limiter, self.circuit_breaker)
        self.fundamentals = FundamentalsManager(self.rate_limiter)
        self.sentiment = AdvancedSentimentAnalyzer(self.rate_limiter, config.finnhub_api_key)
        self.quality = DataQualityGatekeeper()
        self.feature_engineer = FeatureEngineering()
        self.feature_selector = AdvancedFeatureSelector()
        self.model_registry = ModelRegistry(self.redis)
        self.ml_monitor = MLMonitoring(self.redis)
        self.drift_monitor = DriftMonitor(self.redis)
        self.broker = get_broker_adapter(config, None, self.redis)
        self.risk = RiskManager()
        self.position_sizer = KellyPositionSizer()
        self.notifier = MultiChannelNotifier(config, self.rate_limiter)
        self.backtest_engine = BacktestEngine(self.redis)
        self.db_pool = None
        self.running = True
        
        # Redis Streams implementation
        self.stream_key = "fdp:ticker_stream"
        self.consumer_group = "fdp_workers"
        
        # Metrics
        self.metrics = {
            "processed_tickers": 0,
            "failed_tickers": 0,
            "total_signals": 0,
            "api_costs": 0.0
        }
        
        # Setup signal handlers
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._async_signal_handler(s)))
        
        logger.critical("orchestrator_initialized", version="3.1.1")
    
    async def _async_signal_handler(self, signum):
        """Async signal handler for graceful shutdown."""
        logger.critical("shutdown_signal_received", signal=signum)
        self.running = False
        # Cancel all pending tasks
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
    
    async def init_db_pool(self):
        for attempt in range(3):
            try:
                self.db_pool = await asyncpg.create_pool(
                    config.database_url, 
                    min_size=2, 
                    max_size=10,
                    command_timeout=60
                )
                logger.info("db_pool_connected", size=10)
                return
            except Exception as e:
                logger.error("db_pool_retry", attempt=attempt, error=str(e))
                await asyncio.sleep(2 ** attempt)
        raise Exception("DB connection failed after 3 attempts")
    
    async def close_db_pool(self):
        if self.db_pool:
            await self.db_pool.close()
            logger.info("db_pool_closed")
    
    async def check_kill_switch(self) -> bool:
        if not config.kill_switch_enabled:
            return True
        try:
            import aiofiles
            async with aiofiles.open(config.kill_switch_file, 'r') as f:
                content = await f.read()
                if content.strip() == "STOP":
                    logger.critical("kill_switch_active")
                    return False
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.error("kill_switch_check_failed", error=str(e))
        return True
    
    async def init_redis_streams(self):
        """Initialize Redis Streams consumer group."""
        try:
            await self.redis.xgroup_create(self.stream_key, self.consumer_group, id='0', mkstream=True)
            logger.info("redis_streams_group_created", group=self.consumer_group)
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info("redis_streams_group_exists", group=self.consumer_group)
            else:
                raise
    
    async def load_ticker_universe(self) -> pd.DataFrame:
        cache_key = "ticker_universe:v311"
        cached = await self.redis.get(cache_key)
        if cached:
            return pd.read_json(cached)
        
        tasks = [
            self._load_sp500(), 
            self._load_eurostoxx(), 
            self._load_nikkei(), 
            self._load_crypto()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        tickers = [item for result in results if isinstance(result, list) for item in result]
        
        df = pd.DataFrame(tickers, columns=["symbol", "region", "type"])
        df = df.drop_duplicates(subset=["symbol"]).head(config.max_tickers)
        await self.redis.setex(cache_key, 604800, df.to_json())
        logger.info("universe_loaded", tickers=len(df))
        return df
    
    async def _load_sp500(self) -> List[tuple]:
        try:
            sp500 = await asyncio.get_event_loop().run_in_executor(
                None, lambda: pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
            )
            return [(sym, "usa", "stock") for sym in sp500["Symbol"].head(200).tolist()]
        except Exception as e:
            logger.error("sp500_load_failed", error=str(e))
            return []
    
    async def _load_eurostoxx(self) -> List[tuple]:
        try:
            eurostoxx = await asyncio.get_event_loop().run_in_executor(
                None, lambda: pd.read_html("https://en.wikipedia.org/wiki/EURO_STOXX_50")[0]
            )
            return [(sym.replace(".DE", ""), "eu", "stock") for sym in eurostoxx["Ticker"].head(20).tolist()]
        except Exception as e:
            logger.error("eurostoxx_load_failed", error=str(e))
            return []
    
    async def _load_nikkei(self) -> List[tuple]:
        try:
            nikkei = await asyncio.get_event_loop().run_in_executor(
                None, lambda: pd.read_html("https://en.wikipedia.org/wiki/Nikkei_225")[0]
            )
            return [(sym.replace(".T", ""), "asia", "stock") for sym in nikkei["Ticker"].head(10).tolist()]
        except Exception as e:
            logger.error("nikkei_load_failed", error=str(e))
            return []
    
    async def _load_crypto(self) -> List[tuple]:
        return [("BTCUSD", "crypto", "crypto"), ("ETHUSD", "crypto", "crypto")]
    
    async def producer(self, universe: pd.DataFrame):
        """Producer with Redis Streams for durability."""
        for _, row in universe.iterrows():
            if not self.running:
                break
            
            # Atomic budget check
            if not await self.rate_limiter._check_budget_atomic():
                logger.critical("budget_exceeded")
                break
            
            message = {
                "ticker": row["symbol"],
                "region": row["region"],
                "asset_type": row["type"],
                "timestamp": datetime.now().isoformat()
            }
            
            await self.redis.xadd(self.stream_key, message, maxlen=10000)
            logger.debug("ticker_streamed", ticker=row["symbol"])
            await asyncio.sleep(0.1)
        
        # Poison pill message
        await self.redis.xadd(self.stream_key, {"end": "true"})
        logger.info("producer_finished")
    
    async def worker(self, worker_id: int):
        """Worker consuming from Redis Streams."""
        while self.running:
            try:
                # Read from stream with 1s timeout
                messages = await self.redis.xreadgroup(
                    self.consumer_group, 
                    f"consumer_{worker_id}", 
                    {self.stream_key: '>'}, 
                    count=1, 
                    block=1000
                )
                
                if not messages:
                    continue
                
                for stream_name, stream_messages in messages:
                    for message_id, message_data in stream_messages:
                        if message_data.get(b'end') == b'true' or message_data.get('end') == 'true':
                            logger.info("worker_received_poison_pill", worker_id=worker_id)
                            return
                        
                        ticker = message_data.get(b'ticker', b'').decode() or message_data.get('ticker', '')
                        region = message_data.get(b'region', b'').decode() or message_data.get('region', '')
                        asset_type = message_data.get(b'asset_type', b'').decode() or message_data.get('asset_type', '')
                        
                        if not ticker:
                            await self.redis.xack(self.stream_key, self.consumer_group, message_id)
                            continue
                        
                        try:
                            async with asyncio.timeout(120):
                                result = await self.process_ticker_safe(ticker, region, asset_type)
                                
                                if result and result.get("status") == "success":
                                    self.metrics["processed_tickers"] += 1
                                    logger.info("worker_completed", worker_id=worker_id, ticker=ticker)
                                else:
                                    self.metrics["failed_tickers"] += 1
                                
                                # Acknowledge message
                                await self.redis.xack(self.stream_key, self.consumer_group, message_id)
                            
                        except asyncio.TimeoutError:
                            logger.warning("worker_timeout", worker_id=worker_id, ticker=ticker)
                            await self.redis.xack(self.stream_key, self.consumer_group, message_id)
                        except Exception as e:
                            logger.error("worker_error", worker_id=worker_id, ticker=ticker, error=str(e))
                            await self.redis.xack(self.stream_key, self.consumer_group, message_id)
            
            except asyncio.CancelledError:
                logger.info("worker_cancelled", worker_id=worker_id)
                return
            except Exception as e:
                logger.error("worker_unexpected_error", worker_id=worker_id, error=str(e))
                await asyncio.sleep(5)
    
    async def process_ticker_safe(self, ticker: str, region: str, asset_type: str) -> Optional[Dict]:
        for attempt in range(2):
            try:
                return await self.process_ticker(ticker, region, asset_type)
            except Exception as e:
                logger.error("ticker_process_failed", ticker=ticker, attempt=attempt, error=str(e))
                await asyncio.sleep(2 ** attempt)
        self.metrics["failed_tickers"] += 1
        return None
    
    async def process_ticker(self, ticker: str, region: str, asset_type: str) -> Dict:
        """Process ticker with temporal train-test split (no data leakage)."""
        logger.info("processing_ticker", ticker=ticker, region=region, type=asset_type)
        
        # Fetch data
        async with self.market_data as md:
            ohlcv = await md.fetch_ohlcv(ticker, days=730, interval="1d")
        
        # Quality checks
        quality_checks = self.quality.validate_ohlcv(ohlcv)
        if not all(quality_checks.values()):
            logger.warning("data_quality_failed", ticker=ticker, checks=quality_checks)
            return {"status": "failed", "reason": "data_quality"}
        
        async with self.fundamentals as fm:
            fundamentals = await fm.get_latest(ticker)
        
        async with self.sentiment as sa:
            sentiment = await sa.analyze_comprehensive(ticker)
        
        # Feature engineering with temporal split
        X, y = await asyncio.get_event_loop().run_in_executor(
            None, self.feature_engineer.engineer_features, ohlcv, fundamentals, sentiment["composite_score"]
        )
        
        if X.empty or len(X) < 100:
            return {"status": "failed", "reason": "insufficient_data"}
        
        # Temporal train-test split (no shuffling!)
        split_idx = int(len(X) * (1 - config.ml_test_size))
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
        
        # Feature selection
        selection_result = await asyncio.get_event_loop().run_in_executor(
            None, self.feature_selector.select_features, X_train, y_train, "ensemble"
        )
        X_train_selected = selection_result.features
        X_test_selected = X_test[X_train_selected.columns]
        
        # Drift detection
        drift_result = await self.drift_monitor.check(ticker, X_test_selected)
        
        if drift_result["drift_detected"]:
            ref_data_key = f"reference_data:{ticker}"
            ref_data_json = await self.redis.get(ref_data_key)
            if ref_data_json:
                ref_data = pd.read_json(ref_data_json)
                ml_report = await self.ml_monitor.generate_report(ref_data, X_test_selected)
                await self.notifier.send_alert(f"Drift in {ticker}: {json.dumps(ml_report.get('drift', {}))}")
        
        # Model training/loading
        model = StackingEnsemble(ticker)
        last_train_key = f"model:last_train:{ticker}"
        last_train = await self.redis.get(last_train_key)
        retrain_needed = True
        
        if last_train:
            last_train_date = datetime.fromisoformat(last_train)
            retrain_needed = (datetime.now() - last_train_date).days > config.model_retrain_days
        
        if retrain_needed:
            train_results = await asyncio.get_event_loop().run_in_executor(
                None, model.train, X_train_selected, y_train
            )
            # Store reference data for drift
            await self.redis.setex(ref_data_key, 86400*30, X_train_selected.to_json())
            
            await self.redis.setex(last_train_key, 86400, datetime.now().isoformat())
            await self.redis.setex(
                f"ml:metrics:{ticker}",
                86400,
                json.dumps({"accuracy": train_results.get("ensemble", {}).get("f1", 0)})
            )
        
        # Prediction
        prediction = await asyncio.get_event_loop().run_in_executor(
            None, model.predict, X_test_selected.tail(5)
        )
        confidence = np.mean(prediction["probabilities"]) if "probabilities" in prediction else 0.5
        
        if confidence >= config.min_confidence:
            signal = {
                "ticker": ticker,
                "action": "buy" if prediction["direction"] == 1 else "sell",
                "confidence": float(confidence),
                "predicted_return": float(prediction["expected_return"]),
                "timestamp": datetime.now().isoformat()
            }
            
            # Store signal
            await self.redis.lpush("signals:queue", json.dumps(signal))
            await self.redis.sadd("signals:active", ticker)
            await self.redis.publish("signals:new", json.dumps(signal))
            
            # Log to database
            if self.db_pool:
                await self.db_pool.execute(
                    """
                    INSERT INTO signals (ticker, action, confidence, predicted_return, timestamp)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT DO NOTHING
                    """,
                    ticker, signal["action"], signal["confidence"], signal["predicted_return"], signal["timestamp"]
                )
            
            # Audit trail
            await self._log_audit("signal_generated", signal)
            
            logger.critical("signal_generated", **signal)
            
            if config.execution_mode != "alert_only":
                await self.execute_signal(signal, fundamentals)
        
        self.metrics["total_signals"] += 1
        return {"status": "success", "ticker": ticker, "confidence": confidence}
    
    async def _log_audit(self, event_type: str, event_data: Dict):
        """Immutabile audit trail."""
        if self.db_pool:
            await self.db_pool.execute(
                "INSERT INTO audit_log (event_type, event_data, origin_ip) VALUES ($1, $2, $3)",
                event_type, json.dumps(event_data), "127.0.0.1"  # Replace with real IP in production
            )
    
    async def execute_signal(self, signal: Dict, fundamentals: Dict):
        """Execute with correct Kelly calculation."""
        order_key = f"order:executed:{signal['ticker']}:{signal['timestamp']}"
        if await self.redis.exists(order_key):
            logger.info("order_duplicate_suppressed", **signal)
            return
        
        # Get account summary
        account_summary = await self.broker.get_account_summary()
        
        # Correct Kelly calculation: win_probability and win_loss_ratio
        win_probability = signal["confidence"]
        win_loss_ratio = signal["predicted_return"] / fundamentals.get("volatility", 0.02)
        
        position_size = self.position_sizer.calculate_position_size(
            win_probability,
            win_loss_ratio,
            account_summary,
            signal["predicted_return"]
        )
        
        if position_size < 100:  # Min 100$ position
            logger.warning("position_size_too_small", ticker=signal["ticker"], size=position_size)
            return
        
        order = EnhancedOrder(
            symbol=signal["ticker"],
            action=signal["action"],
            quantity=position_size,
            order_type="limit",
            limit_price=signal.get("current_price", 0),
            idempotency_key=order_key,
            metadata={"signal": signal}
        )
        
        # Risk check
        risk_check = self.risk.validate_order(order)
        if not risk_check["allowed"]:
            logger.warning("order_risk_rejected", ticker=signal["ticker"], reason=risk_check["reason"])
            return
        
        # Place order
        order_id = await self.broker.place_order(order)
        await self.redis.setex(order_key, 86400, order_id)
        
        # Publish order status
        await self.redis.publish("orders:status", json.dumps({
            "order_id": order_id,
            "status": "submitted",
            "ticker": signal["ticker"]
        }))
        
        await self._log_audit("order_submitted", {
            "order_id": order_id,
            "signal": signal,
            "position_size": position_size
        })
        
        logger.critical("order_submitted", order_id=order_id, **signal)
    
    async def sync_task(self):
        """Background sync task."""
        while self.running:
            try:
                await self.broker.sync_orders()
                await asyncio.sleep(5)
            except Exception as e:
                logger.error("sync_task_error", error=str(e))
                await asyncio.sleep(30)
    
    async def metrics_reporter(self):
        """Report metrics to Prometheus every 60s."""
        while self.running:
            try:
                # Export metrics to Redis for Prometheus exporter
                await self.redis.hset("fdp:metrics", mapping={
                    "processed_tickers": self.metrics["processed_tickers"],
                    "failed_tickers": self.metrics["failed_tickers"],
                    "total_signals": self.metrics["total_signals"],
                    "api_costs": self.metrics["api_costs"]
                })
                await asyncio.sleep(60)
            except Exception as e:
                logger.error("metrics_reporter_error", error=str(e))
                await asyncio.sleep(60)
    
    async def main(self):
        if not await self.check_kill_switch():
            logger.critical("kill_switch_prevented_startup")
            return
        
        await self.init_db_pool()
        await self.init_redis_streams()
        
        # Start background tasks
        num_workers = getattr(config, 'max_concurrent_workers', 20)
        worker_tasks = [asyncio.create_task(self.worker(i)) for i in range(num_workers)]
        sync_task = asyncio.create_task(self.sync_task())
        metrics_task = asyncio.create_task(self.metrics_reporter())
        
        # Load universe
        universe = await self.load_ticker_universe()
        logger.critical("universe_loaded", tickers=len(universe))
        
        # Run producer
        producer_task = asyncio.create_task(self.producer(universe))
        
        # Wait for completion
        await producer_task
        await self.ticker_queue.join()
        
        # Graceful shutdown
        logger.info("shutting_down_tasks")
        for task in worker_tasks:
            task.cancel()
        
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        sync_task.cancel()
        metrics_task.cancel()
        
        # Final metrics report
        logger.critical("final_metrics", metrics=self.metrics)
        
        await self.close_db_pool()
        logger.critical("orchestrator_shutdown_complete")

if __name__ == "__main__":
    try:
        orchestrator = FinDashProOrchestrator()
        asyncio.run(orchestrator.main())
    except ConfigValidationError as e:
        print(f"Config Error: {', '.join(e.errors)}")
        sys.exit(1)
    except Exception as e:
        logger.critical("fatal_error", error=str(e), traceback=traceback.format_exc())
        sys.exit(1)
