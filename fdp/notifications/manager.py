import asyncio
from typing import Dict, Any, List, Optional
import aiohttp
import structlog
from fdp.core.config import config

logger = structlog.get_logger()

class BaseNotifier:
    """Base notification class."""
    
    async def send_alert(self, message: str, **kwargs):
        raise NotImplementedError

class TelegramNotifier(BaseNotifier):
    """Telegram notifier."""
    
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def send_alert(self, message: str, **kwargs):
        """Send Telegram alert."""
        if not self.token or not self.chat_id:
            logger.warning("telegram_not_configured")
            return
        
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message[:4000],  # Telegram limit
            "parse_mode": "HTML"
        }
        
        try:
            async with self.session.post(url, json=payload, timeout=10) as resp:
                if resp.status != 200:
                    logger.error("telegram_send_failed", status=resp.status, text=await resp.text())
                else:
                    logger.info("telegram_sent")
        except Exception as e:
            logger.error("telegram_error", error=str(e))

class DiscordNotifier(BaseNotifier):
    """Discord webhook notifier."""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def send_alert(self, message: str, **kwargs):
        """Send Discord alert."""
        if not self.webhook_url:
            logger.warning("discord_not_configured")
            return
        
        payload = {"content": message[:2000]}  # Discord limit
        
        try:
            async with self.session.post(self.webhook_url, json=payload, timeout=10) as resp:
                if resp.status not in [200, 204]:
                    logger.error("discord_send_failed", status=resp.status)
                else:
                    logger.info("discord_sent")
        except Exception as e:
            logger.error("discord_error", error=str(e))

class MultiChannelNotifier:
    """Multi-channel notification manager."""
    
    def __init__(self, config: Any, rate_limiter: Any):
        self.config = config
        self.rate_limiter = rate_limiter
        self.notifiers: List[BaseNotifier] = []
        self._init_notifiers()
    
    def _init_notifiers(self):
        """Initialize enabled notifiers."""
        if self.config.telegram_token and self.config.telegram_chat_id:
            self.notifiers.append(TelegramNotifier(
                self.config.telegram_token,
                self.config.telegram_chat_id
            ))
        
        if self.config.discord_webhook:
            self.notifiers.append(DiscordNotifier(self.config.discord_webhook))
    
    async def send_alert(self, message: str, **kwargs):
        """Send alert to all channels."""
        if not self.notifiers:
            logger.warning("no_notifiers_configured")
            return
        
        # Rate limit notifications (cost ≈ 0.001 per alert)
        await self.rate_limiter.record_spend("notifications", 0.001)
        
        tasks = []
        for notifier in self.notifiers:
            async with notifier:  # Ensure session lifecycle
                tasks.append(notifier.send_alert(message, **kwargs))
        
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("alerts_sent", channels=len(self.notifiers))
    
    def add_notifier(self, notifier: BaseNotifier):
        """Add custom notifier."""
        self.notifiers.append(notifier)
