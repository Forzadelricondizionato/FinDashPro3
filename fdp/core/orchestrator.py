import asyncio
import signal
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import traceback
import numpy as np
import pandas as pd
import redis.asyncio as redis
import asyncpg
import structlog
from prometheus_client import start_http_server

from fdp.core.config import config
from fdp.core.rate_limiter import TokenBucketRateLimiter
from fdp.core.circuit_breaker import CircuitBreaker
from fdp.core.vault_client import VaultClient
from fdp.core.audit_logger import AuditLogger
from fdp.core.prometheus_metrics import METRICS
from fdp.security.mtls_config import get_ssl_context
from fdp.tracing.jaeger_tracer import trace_function, tracer
from fdp.api.health import router as health_router
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

logger = structlog.get_logger()

class FinDashProOrchestrator:
    def __init__(self):
        self.vault_client = VaultClient()
        self.redis = redis.from_url(config.redis_url, decode_responses=True)
        self.rate_limiter = TokenBucketRateLimiter(self.redis, config.daily_api_budget)
        self.circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=300)
        self.market_data = MultiSourceMarketDataManager(self.rate_limiter, self.circuit_breaker)
        self.fundamentals = FundamentalsManager(self.rate_limiter)
        self.sentiment = AdvancedSentimentAnalyzer(self.rate_limiter, self.vault_client.read_secret("finnhub_key"))
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
        self.audit_logger = AuditLogger(None)
        self.db_pool = None
        self.running = True
        self.stream_key = "fdp:ticker_stream"
        self.consumer_group = "fdp_workers"
        self.metrics = {
            "processed_tickers": 0,
            "failed_tickers": 0,
            "total_signals": 0,
            "api_costs": 0.0
        }
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._async_signal_handler(s)))
        
        start_http_server(8000)
        logger.critical("orchestrator_initialized", version="3.2.0")
    
    async def _async_signal_handler(self, signum):
        logger.critical("shutdown_signal_received", signal=signum)
        self.running = False
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
    
    async def init_db_pool(self):
        ssl_ctx = get_ssl_context(server=False, for_client="postgres") if config.mtls_enabled else None
        for attempt in range(3):
            try:
                self.db_pool = await asyncpg.create_pool(
                    config.database_url,
                    min_size=2,
                    max_size=20,
                    command_timeout=60,
                    ssl=ssl_ctx
                )
                self.audit_logger.pool = self.db_pool
                logger.info("db_pool_connected", size=20)
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
        try:
            await self.redis.xgroup_create(self.stream_key, self.consumer_group, id='0', mkstream=True)
            logger.info("redis_streams_group_created", group=self.consumer_group)
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info("redis_streams_group_exists", group=self.consumer_group)
            else:
                raise
    
    async def load_ticker_universe(self) -> pd.DataFrame:
        cache_key = "ticker_universe:v320"
        cached = await self.redis.get(cache_key)
        if cached:
            return pd.read_json(cached)
        
        tasks = [
            self._load_sp500(),
            self._load_eurostoxx(),
            self._load_nikkei(),
            self._load_crypto(),
            self._load_mib40()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        tickers = []
        for result in results:
            if isinstance(result, list):
                tickers.extend(result)
        
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
            return [(sym.replace(".DE", ""), "eu", "stock") for sym in eurostoxx["Ticker"].head(30).tolist()]
        except Exception as e:
            logger.error("eurostoxx_load_failed", error=str(e))
            return []
    
    async def _load_nikkei(self) -> List[tuple]:
        try:
            nikkei = await asyncio.get_event_loop().run_in_executor(
                None, lambda: pd.read_html("https://en.wikipedia.org/wiki/Nikkei_225")[0]
            )
            return [(sym.replace(".T", ""), "asia", "stock") for sym in nikkei["Ticker"].head(20).tolist()]
        except Exception as e:
            logger.error("nikkei_load_failed", error=str(e))
            return []
    
    async def _load_crypto(self) -> List[tuple]:
        return [("BTCUSD", "crypto", "crypto"), ("ETHUSD", "crypto", "crypto"), ("SOLUSD", "crypto", "crypto")]
    
    async def _load_mib40(self) -> List[tuple]:
        try:
            mib = await asyncio.get_event_loop().run_in_executor(
                None, lambda: pd.read_html("https://en.wikipedia.org/wiki/FTSE_MIB")[0]
            )
            return [(sym.replace(".MI", ""), "italy", "stock") for sym in mib["Ticker"].head(40).tolist()]
        except Exception as e:
            logger.error("mib40_load_failed", error=str(e))
            return []
    
    async def producer(self, universe: pd.DataFrame):
        for _, row in universe.iterrows():
            if not self.running:
                break
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
        
        await self.redis.xadd(self.stream_key, {"end": "true"})
        logger.info("producer_finished")
    
    async def worker(self, worker_id: int):
        while self.running:
            try:
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
                            logger.info("worker_poison_pill", worker_id=worker_id)
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
                                    METRICS["processed_tickers"].inc()
                                    logger.info("worker_completed", worker_id=worker_id, ticker=ticker)
                                else:
                                    self.metrics["failed_tickers"] += 1
                                    METRICS["failed_tickers"].inc()
                                
                                await self.redis.xack(self.stream_key, self.consumer_group, message_id)
                        except asyncio.TimeoutError:
                            logger.warning("worker_timeout", worker_id=worker_id, ticker=ticker)
                            await self.redis.xadd("fdp:failed_ticker", {"ticker": ticker, "reason": "timeout"})
                        except Exception as e:
                            logger.error("worker_error", worker_id=worker_id, ticker=ticker, error=str(e))
                            await self.redis.xadd("fdp:failed_ticker", {"ticker": ticker, "reason": str(e)})
            except Exception as e:
                logger.error("worker_exception", worker_id=worker_id, error=str(e))
                await asyncio.sleep(5)
    
    @trace_function("process_ticker_safe")
    async def process_ticker_safe(self, ticker: str, region: str, asset_type: str) -> Dict[str, Any]:
        with tracer.start_active_span("process_ticker") as scope:
            scope.span.set_tag("ticker", ticker)
            try:
                if not await self.check_kill_switch():
                    return {"status": "killed"}
                
                await self.rate_limiter.acquire("processing", f"ticker:{ticker}", 0)
                
                ohlcv = await self.market_data.fetch_ohlcv(ticker, datetime.now() - timedelta(days=365), datetime.now())
                if ohlcv.empty:
                    return {"status": "failed", "reason": "No OHLCV data"}
                
                if not self.quality.validate_ohlcv(ohlcv):
                    return {"status": "failed", "reason": "Data quality failed"}
                
                fundamentals = await self.fundamentals.get_latest(ticker)
                sentiment = await self.sentiment.analyze_comprehensive(ticker)
                
                X, y = self.feature_engineer.engineer_features(ohlcv, fundamentals, sentiment)
                if X.empty:
                    return {"status": "failed", "reason": "Feature engineering failed"}
                
                selected_features = self.feature_selector.select_features(X, y)
                model = self.model_registry.get_model(ticker)
                prediction = model.predict(selected_features)
                
                if prediction["confidence"] < config.min_confidence:
                    return {"status": "skipped", "reason": "Confidence below threshold"}
                
                drift_result = await self.drift_monitor.check(ticker, X)
                if drift_result["drift_detected"]:
                    await self.notifier.send_alert(f"Drift detected on {ticker}: {drift_result['warnings']}")
                
                account = await self.broker.get_account_summary()
                position_size = self.position_sizer.calculate_position_size(
                    win_probability=prediction["probability"],
                    win_loss_ratio=2.0,
                    account_summary=account,
                    edge=prediction["expected_return"]
                )
                
                signal = {
                    "ticker": ticker,
                    "action": prediction["direction"],
                    "price": prediction.get("target_price", 0),
                    "confidence": prediction["confidence"],
                    "position_size": position_size,
                    "timestamp": datetime.now().isoformat(),
                    "drift_detected": drift_result["drift_detected"]
                }
                
                await self.redis.lpush("signals:queue", signal)
                await self.audit_logger.log("signal_generated", "system", ticker, prediction["direction"], signal)
                
                METRICS["signals_total"].inc()
                self.metrics["total_signals"] += 1
                
                scope.span.set_tag("error", False)
                return {"status": "success", "signal": signal}
            except Exception as e:
                logger.error("process_ticker_error", ticker=ticker, error=str(e))
                scope.span.set_tag("error", True)
                scope.span.log_kv({'error': str(e)})
                return {"status": "failed", "reason": str(e)}
    
    async def shutdown(self):
        self.running = False
        await self.close_db_pool()
        await self.broker.disconnect()
        await self.redis.close()
