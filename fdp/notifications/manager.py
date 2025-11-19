import asyncio
import aiohttp
from typing import Dict, Any, List
import structlog

logger = structlog.get_logger()

class MultiChannelNotifier:
    def __init__(self, config, rate_limiter):
        self.config = config
        self.rate_limiter = rate_limiter
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def send_alert(self, message: str, level: str = "info", channels: List[str] = None):
        if channels is None:
            channels = ["telegram", "discord"]
        tasks = []
        if "telegram" in channels and self.config.telegram_token:
            tasks.append(self._send_telegram(message))
        if "discord" in channels and self.config.discord_webhook:
            tasks.append(self._send_discord(message, level))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_telegram(self, message: str):
        if not self.config.telegram_token or not self.config.telegram_chat_id:
            return
        url = f"https://api.telegram.org/bot{self.config.telegram_token}/sendMessage"
        payload = {"chat_id": self.config.telegram_chat_id, "text": message}
        async with self.session.post(url, json=payload) as resp:
            if resp.status == 200:
                logger.info("telegram_message_sent")
            else:
                logger.error("telegram_send_failed", status=resp.status)

    async def _send_discord(self, message: str, level: str):
        if not self.config.discord_webhook:
            return
        colors = {"info": 3447003, "warning": 16776960, "critical": 15158332}
        payload = {
            "content": "FinDashPro Alert",
            "embeds": [{"title": level.upper(), "description": message, "color": colors.get(level, 3447003)}]
        }
        async with self.session.post(self.config.discord_webhook, json=payload) as resp:
            if resp.status == 204:
                logger.info("discord_message_sent")
            else:
                logger.error("discord_send_failed", status=resp.status)

    async def send_signal_notification(self, signal: Dict[str, Any]):
        message = f"🚀 Signal: {signal['ticker']} {signal['action']} (confidence: {signal['confidence']:.2f})"
        await self.send_alert(message, level="info", channels=["telegram"])

    async def send_risk_warning(self, warning: str):
        await self.send_alert(f"⚠️ Risk Warning: {warning}", level="warning", channels=["discord", "telegram"])

    async def send_drift_alert(self, ticker: str, drift_score: float):
        message = f"🚨 Drift detected in {ticker} (score: {drift_score:.4f})"
        await self.send_alert(message, level="critical", channels=["discord", "telegram"])
