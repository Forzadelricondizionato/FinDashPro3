from enum import Enum
from dataclasses import dataclass
from typing import Optional
import structlog
from fdp.core.config import config

logger = structlog.get_logger()

class ServiceTier(Enum):
    FREE = "free"
    PREMIUM = "premium"
    DISABLED = "disabled"

@dataclass
class ServiceConfig:
    tier: ServiceTier
    free_limit: Optional[int] = None
    premium_endpoint: Optional[str] = None
    usage_counter: int = 0

    def is_available(self) -> bool:
        if self.tier == ServiceTier.DISABLED:
            return False
        if self.tier == ServiceTier.FREE and self.free_limit:
            return self.usage_counter < self.free_limit
        return True

    def increment_usage(self):
        self.usage_counter += 1

class TieredServiceManager:
    def __init__(self):
        self.services = {}
        self._initialize_services()

    def _initialize_services(self):
        for provider in ["yahoo", "alpha", "tiingo", "finnhub", "polygon", "fmp"]:
            tier_str = getattr(config, f"tier_{provider}", "free")
            tier = ServiceTier(tier_str)
            free_limit = getattr(config, f"free_limit_{provider}", None)
            premium_endpoint = getattr(config, f"premium_endpoint_{provider}", None)
            
            self.services[provider] = ServiceConfig(
                tier=tier,
                free_limit=free_limit,
                premium_endpoint=premium_endpoint
            )

    def get_service_config(self, provider: str) -> ServiceConfig:
        return self.services.get(provider, ServiceConfig(ServiceTier.DISABLED))

    async def check_and_record_usage(self, provider: str) -> bool:
        config = self.get_service_config(provider)
        if not config.is_available():
            return False
        
        config.increment_usage()
        
        if config.tier == ServiceTier.FREE and config.usage_counter >= config.free_limit * 0.9:
            logger.warning("service_near_limit", provider=provider, usage=config.usage_counter, limit=config.free_limit)
        
        return True

    def reset_daily_counters(self):
        for config in self.services.values():
            config.usage_counter = 0
