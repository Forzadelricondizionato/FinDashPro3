import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, Optional
import redis.asyncio as redis
import structlog

logger = structlog.get_logger()

class BudgetMonitor:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.budget_key = "budget:daily_spent"
        self.alert_threshold = 0.85
        self.critical_threshold = 0.95
        self.hourly_pattern_key = "budget:hourly_pattern"

    async def get_current_spend(self) -> float:
        spend = await self.redis.get(self.budget_key)
        return float(spend) if spend else 0.0

    async def get_provider_costs(self) -> Dict[str, float]:
        costs = await self.redis.zrange("budget:by_provider", 0, -1, withscores=True)
        return {provider.decode() if isinstance(provider, bytes) else provider: cost for provider, cost in costs}

    async def check_budget_critical(self) -> bool:
        spend = await self.get_current_spend()
        is_critical = spend >= config.daily_api_budget * self.critical_threshold
        if is_critical:
            await self.redis.publish("budget:critical", json.dumps({
                "spent": spend,
                "budget": config.daily_api_budget,
                "timestamp": datetime.now().isoformat()
            }))
            logger.critical("budget_threshold_exceeded", spent=spend, threshold=self.critical_threshold)
        return is_critical

    async def record_hourly_spend(self):
        spend = await self.get_current_spend()
        hour = datetime.now().hour
        await self.redis.lpush(self.hourly_pattern_key, spend)
        await self.redis.ltrim(self.hourly_pattern_key, 0, 23)
        logger.debug("hourly_spend_recorded", hour=hour, spend=spend)

    async def get_cost_projection(self) -> Dict[str, float]:
        hourly_pattern = await self.redis.lrange(self.hourly_pattern_key, 0, -1)
        current_hour = datetime.now().hour
        if not hourly_pattern:
            return {"projected": 0.0, "remaining_hours": 0, "avg_hourly": 0.0}
        pattern = []
        for x in hourly_pattern:
            try:
                if isinstance(x, bytes):
                    pattern.append(float(x.decode()))
                else:
                    pattern.append(float(x))
            except (ValueError, TypeError):
                continue
        avg_hourly = sum(pattern) / len(pattern) if pattern else 0.0
        remaining_hours = 24 - current_hour
        return {
            "projected": await self.get_current_spend() + (avg_hourly * remaining_hours),
            "remaining_hours": remaining_hours,
            "avg_hourly": avg_hourly
        }
