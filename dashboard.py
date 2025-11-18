import streamlit as st
import asyncio
import json
import pandas as pd
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import sys
from pathlib import Path
import redis.asyncio as redis
import structlog

sys.path.append(str(Path(__file__).parent))

from fdp.core.config import config
from fdp.core.rate_limiter import TokenBucketRateLimiter
from fdp.trading.broker_adapter_enhanced import get_broker_adapter
from fdp.notifications.manager import MultiChannelNotifier

st.set_page_config(page_title="FinDashPro ML-Max 3.1.1", page_icon="📈", layout="wide")

logger = structlog.get_logger()

class DashboardApp:
    def __init__(self):
        self.redis = None
        self.broker = None
        self.notifier = None
        self.websocket_task = None
        self.realtime_data = {}
        self.running = True
    
    async def init(self):
        self.redis = redis.from_url(config.redis_url, decode_responses=True)
        self.notifier = MultiChannelNotifier(config, TokenBucketRateLimiter(self.redis, config.daily_api_budget))
        self.broker = get_broker_adapter(config, self.notifier, self.redis)
        await self.broker.connect()
        logger.info("dashboard_initialized")
    
    async def shutdown(self):
        self.running = False
        if self.websocket_task:
            self.websocket_task.cancel()
        if self.broker:
            await self.broker.graceful_shutdown()
        if self.redis:
            await self.redis.close()
        logger.info("dashboard_shutdown")
    
    @st.cache_data(ttl=60)
    def load_historical_data(_self, ticker: str) -> pd.DataFrame:
        """Load historical data with caching."""
        # In production, this would query the database
        return pd.DataFrame({
            'date': pd.date_range(start='2023-01-01', periods=365, freq='D'),
            'close': np.random.randn(365).cumsum() + 100,
            'volume': np.random.randint(1e6, 5e6, 365)
        })
    
    def render_header(self):
        """Render real-time metrics header."""
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            portfolio_value = self.redis.get("metrics:portfolio_value") or "1234567"
            st.metric("Portfolio Value", f"${float(portfolio_value):,.2f}")
        
        with col2:
            daily_pnl = self.redis.get("metrics:daily_pnl") or "12345"
            st.metric("Daily P&L", f"${float(daily_pnl):,.2f}")
        
        with col3:
            active_signals = self.redis.scard("signals:active") or 0
            st.metric("Active Signals", int(active_signals))
        
        with col4:
            api_spend = self.redis.get("budget:daily_spent") or 0
            budget_pct = (float(api_spend) / config.daily_api_budget) * 100
            st.metric("API Budget", f"{budget_pct:.1f}%")
    
    async def websocket_listener(self):
        """Listen for real-time updates via Redis Pub/Sub."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("signals:new", "orders:status", "dashboard:updates")
        
        try:
            while self.running:
                message = await pubsub.get_message(timeout=1.0)
                if message and message['type'] == 'message':
                    data = json.loads(message['data'])
                    self.realtime_data[data.get('channel', message['channel'])] = data
                    st.session_state['last_update'] = datetime.now()
                await asyncio.sleep(0.01)
        except Exception as e:
            logger.error("websocket_listener_error", error=str(e))
    
    def render_realtime_panel(self):
        """Render real-time notifications panel."""
        if 'last_update' not in st.session_state:
            st.session_state['last_update'] = datetime.now()
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.info(f"Last update: {st.session_state['last_update'].strftime('%H:%M:%S')}")
        
        with col2:
            if st.button("🔄 Force Refresh"):
                st.cache_data.clear()
                st.rerun()
        
        # Show toast for new signals
        if "signals:new" in self.realtime_data:
            signal = self.realtime_data["signals:new"]
            st.toast(
                f"🚀 New Signal: {signal['ticker']} {signal['action']} "
                f"@ ${signal.get('price', 'N/A'):.2f}" if isinstance(signal.get('price'), (int, float)) else "N/A",
                icon="📈"
            )
    
    def render_ticker_selector(self) -> str:
        """Ticker selector with caching."""
        @st.cache_data(ttl=3600)
        def load_universe():
            try:
                # Try to load from Redis first
                redis_client = redis.from_url(config.redis_url, decode_responses=True)
                cached = redis_client.get("ticker_universe:v311")
                if cached:
                    df = pd.read_json(cached)
                    return df['symbol'].tolist()[:50]
            except:
                pass
            return ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'NVDA', 'META', 'AMZN', 'NFLX']
        
        return st.selectbox("Select Ticker", options=load_universe())
    
    def render_chart(self, ticker: str, df: pd.DataFrame):
        """Render interactive chart."""
        fig = make_subplots(
            rows=2, cols=1, 
            subplot_titles=("Price & Volume", "RSI & Indicators"),
            row_heights=[0.7, 0.3],
            vertical_spacing=0.05
        )
        
        # Price chart
        fig.add_trace(
            go.Scattergl(x=df['date'], y=df['close'], mode='lines', name='Close'),
            row=1, col=1
        )
        fig.add_trace(
            go.Bar(x=df['date'], y=df['volume'], name='Volume', opacity=0.5),
            row=1, col=1
        )
        
        # RSI (calculate here for demo)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        fig.add_trace(
            go.Scattergl(x=df['date'], y=rsi, mode='lines', name='RSI'),
            row=2, col=1
        )
        fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)
        
        fig.update_layout(
            height=600, 
            title=f"{ticker} Analytics - FinDashPro ML-Max",
            template="plotly_dark",
            showlegend=True
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_ml_dashboard(self):
        """ML performance dashboard."""
        st.header("🔬 ML Model Performance")
        
        tab1, tab2, tab3 = st.tabs(["Drift Detection", "Feature Importance", "Model Registry"])
        
        with tab1:
            drift_tickers = self.redis.keys("drift:*") or []
            if drift_tickers:
                st.warning(f"🚨 Drift detected in {len(drift_tickers)} tickers")
                for key in drift_tickers[:10]:
                    ticker = key.split(":")[1]
                    data = self.redis.hgetall(key)
                    st.json(data)
            else:
                st.success("✅ No drift detected")
        
        with tab2:
            importance_json = self.redis.get("ml:feature_importance")
            if importance_json:
                try:
                    importance = pd.read_json(importance_json)
                    st.bar_chart(importance.set_index('feature'))
                except:
                    st.info("Feature importance not available")
            else:
                st.info("No feature importance data")
        
        with tab3:
            model_keys = self.redis.keys("model:*") or []
            st.metric("Models Tracked", len(model_keys))
            for key in model_keys[:5]:
                model_data = self.redis.get(key)
                if model_data:
                    st.json(json.loads(model_data))
    
    async def render_notifications_panel(self):
        """Notifications panel."""
        st.subheader("🔔 Notifications")
        
        if not self.redis:
            st.info("Redis not connected")
            return
        
        alerts = await self.redis.lrange("notifications:recent", 0, 9)
        
        if not alerts:
            st.info("No recent notifications")
            return
        
        for alert_json in alerts:
            try:
                alert = json.loads(alert_json)
                with st.expander(f"{alert.get('timestamp', '')}: {alert.get('title', '')}"):
                    st.json(alert)
            except:
                st.write(alert_json)
    
    def render_settings(self):
        """Safe settings view."""
        st.subheader("⚙️ Settings (Secrets Redacted)")
        
        safe_config = {k: v for k, v in config.__dict__.items() 
                       if not any(secret in k.lower() for secret in ['key', 'secret', 'token', 'password'])}
        st.json(safe_config)
    
    def render_metrics(self):
        """System metrics."""
        st.subheader("📊 System Metrics")
        
        metrics = self.redis.hgetall("fdp:metrics") or {}
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Processed Tickers", int(metrics.get('processed_tickers', 0)))
        with col2:
            st.metric("Failed Tickers", int(metrics.get('failed_tickers', 0)))
        with col3:
            st.metric("Total Signals", int(metrics.get('total_signals', 0)))
        with col4:
            st.metric("API Costs", f"€{float(metrics.get('api_costs', 0)):.2f}")
    
    async def run(self):
        await self.init()
        
        try:
            # Start websocket listener
            self.websocket_task = asyncio.create_task(self.websocket_listener())
            
            with st.sidebar:
                st.title("FinDashPro ML-Max 3.1.1")
                ticker = self.render_ticker_selector()
                await self.render_notifications_panel()
                
                st.markdown("---")
                st.caption("System")
                if st.button("Clear Cache"):
                    st.cache_data.clear()
                    st.cache_resource.clear()
                    st.success("Cache cleared!")
                
                st.markdown("---")
                st.caption("Mode")
                st.code(f"{config.execution_mode.upper()}", language="text")
            
            self.render_header()
            self.render_realtime_panel()
            
            # Tabs
            tab1, tab2, tab3, tab4 = st.tabs(["📈 Chart", "🤖 ML Models", "📊 Metrics", "🔒 Settings"])
            
            with st.spinner("Loading data..."):
                df = self.load_historical_data(ticker)
            
            with tab1:
                self.render_chart(ticker, df)
            
            with tab2:
                self.render_ml_dashboard()
            
            with tab3:
                self.render_metrics()
            
            with tab4:
                self.render_settings()
            
            st.caption("FinDashPro ML-Max 3.1.1 Enterprise | © 2024")
            
            # Auto refresh
            if st.session_state.get('auto_refresh', False):
                await asyncio.sleep(10)
                st.rerun()
        
        finally:
            await self.shutdown()

if __name__ == "__main__":
    app = DashboardApp()
    asyncio.run(app.run())
