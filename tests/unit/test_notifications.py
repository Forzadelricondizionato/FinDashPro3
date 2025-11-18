import pytest
from unittest.mock import AsyncMock, patch
from fdp.notifications.manager import (
    TelegramNotifier,
    DiscordNotifier,
    MultiChannelNotifier,
    BaseNotifier
)
from fdp.core.rate_limiter import TokenBucketRateLimiter

pytestmark = pytest.mark.asyncio

class TestTelegramNotifier:
    async def test_telegram_send_success(self):
        """Test successful Telegram alert."""
        notifier = TelegramNotifier("test_token", "123456")
        
        mock_response = {"ok": True}
        
        with patch.object(notifier, 'session') as mock_session:
            mock_post = AsyncMock()
            mock_post.return_value.status = 200
            mock_post.return_value.json = AsyncMock(return_value=mock_response)
            mock_session.post = mock_post
            
            async with notifier:
                await notifier.send_alert("Test alert")
                
                mock_post.assert_called_once()
    
    async def test_telegram_not_configured(self):
        """Test no alert when not configured."""
        notifier = TelegramNotifier("", "")
        
        # Should log warning but not raise
        await notifier.send_alert("Test")
        
        # No session created if not configured
        assert notifier.session is None

class TestDiscordNotifier:
    async def test_discord_send_success(self):
        """Test successful Discord alert."""
        notifier = DiscordNotifier("https://discord.com/api/webhooks/test")
        
        with patch.object(notifier, 'session') as mock_session:
            mock_post = AsyncMock()
            mock_post.return_value.status = 204
            mock_session.post = mock_post
            
            async with notifier:
                await notifier.send_alert("Test alert")
                
                mock_post.assert_called_once()
    
    async def test_discord_not_configured(self):
        """Test no alert when not configured."""
        notifier = DiscordNotifier("")
        
        await notifier.send_alert("Test")
        
        assert notifier.session is None

class TestMultiChannelNotifier:
    async def test_multi_channel_send(self, mock_config, redis_client):
        """Test sending to multiple channels."""
        rate_limiter = TokenBucketRateLimiter(redis_client, budget=100)
        
        notifier = MultiChannelNotifier(mock_config, rate_limiter)
        
        # Mock notifiers
        mock_telegram = AsyncMock()
        mock_discord = AsyncMock()
        notifier.notifiers = [mock_telegram, mock_discord]
        
        await notifier.send_alert("Multi channel test")
        
        mock_telegram.send_alert.assert_called_once()
        mock_discord.send_alert.assert_called_once()
        
        # Check rate limit was recorded
        spent = await redis_client.get("budget:daily_spent")
        assert float(spent or 0) > 0
    
    def test_add_custom_notifier(self, mock_config, redis_client):
        """Test adding custom notifier."""
        rate_limiter = TokenBucketRateLimiter(redis_client, budget=100)
        notifier = MultiChannelNotifier(mock_config, rate_limiter)
        
        class MockNotifier(BaseNotifier):
            async def send_alert(self, message: str, **kwargs):
                pass
        
        custom = MockNotifier()
        notifier.add_notifier(custom)
        
        assert len(notifier.notifiers) == 1  # Only custom, no tele/discord

async def test_notification_rate_limiting(redis_client):
    """Test notifications are rate limited."""
    config_mock = AsyncMock()
    config_mock.telegram_token = "test"
    config_mock.telegram_chat_id = "123"
    config_mock.discord_webhook = None
    
    rate_limiter = TokenBucketRateLimiter(redis_client, budget=0.1)  # Low budget
    
    notifier = MultiChannelNotifier(config_mock, rate_limiter)
    
    # Should fail due to budget
    with pytest.raises(Exception):  # BudgetExceededError
        for _ in range(10):  # Exceed budget
            await notifier.send_alert("Test")
