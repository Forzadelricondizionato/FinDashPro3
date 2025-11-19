import asyncio
import time
from collections import defaultdict
import redis.asyncio as redis
import structlog

logger = structlog.get_logger()

class AdaptiveTokenBucketRateLimiter:
    def __init__(self, redis_client: redis.Redis, budget: float = 5.0):
        self.redis = redis_client
        self.budget = budget
        self.daily_spend_key = "budget:daily_spent"
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._lock_cleanup_threshold = 1000
        self.adaptive_mode = True
        self.performance_window = defaultdict(list)

    async def acquire(self, provider: str, key: str, limit_per_min: int):
        budget_ok = await self._check_budget_atomic()
        if not budget_ok:
            raise BudgetExceededError(f"Daily budget exceeded: {self.budget}€")
        lock_key = f"lock:{provider}:{key}"
        bucket_key = f"rate_limit:{provider}:{key}"
        async with self._locks[lock_key]:
            if len(self._locks) > self._lock_cleanup_threshold:
                self._locks.clear()
            now = time.time()
            pipe = self.redis.pipeline()
            pipe.hget(bucket_key, "tokens")
            pipe.hget(bucket_key, "last_refill")
            tokens, last_refill = await pipe.execute()
            if tokens is None:
                tokens = limit_per_min
                last_refill = now
            else:
                tokens = float(tokens)
                last_refill = float(last_refill) if last_refill else now
            elapsed = (now - last_refill) / 60.0
            adaptive_rate = await self._calculate_adaptive_rate(provider, limit_per_min)
            new_tokens = min(adaptive_rate, tokens + elapsed * adaptive_rate)
            if new_tokens < 1.0:
                wait_time = (1.0 - new_tokens) / adaptive_rate * 60
                logger.warning("rate_limit_wait", provider=provider, key=key, wait=wait_time)
                await asyncio.sleep(wait_time)
                now = time.time()
                elapsed = (now - last_refill) / 60.0
                new_tokens = min(1.0, elapsed * adaptive_rate)
            pipe = self.redis.pipeline()
            pipe.hset(bucket_key, mapping={"tokens": new_tokens - 1, "last_refill": now})
            pipe.expire(bucket_key, 3600)
            await pipe.execute()

    async def _calculate_adaptive_rate(self, provider: str, base_rate: int) -> int:
        if not self.adaptive_mode:
            return base_rate
        recent_performances = self.performance_window[provider]
        if len(recent_performances) < 5:
            return base_rate
        avg_latency = sum(p['latency'] for p in recent_performances) / len(recent_performances)
        if avg_latency < 0.5:
            return int(base_rate * 1.2)
        elif avg_latency > 2.0:
            return int(base_rate * 0.8)
        return base_rate

    async def record_performance(self, provider: str, latency: float, success: bool):
        self.performance_window[provider].append({
            'latency': latency,
            'success': success,
            'timestamp': time.time()
        })
        if len(self.performance_window[provider]) > 20:
            self.performance_window[provider].pop(0)

    async def _check_budget_atomic(self) -> bool:
        script = """
        local spent = redis.call('GET', KEYS[1]) or '0'
        local budget = tonumber(ARGV[1])
        return tonumber(spent) < budget
        """
        return bool(await self.redis.eval(script, 1, self.daily_spend_key, self.budget))

    async def record_spend(self, provider: str, cost: float):
        pipe = self.redis.pipeline()
        pipe.incrbyfloat(self.daily_spend_key, cost)
        pipe.expire(self.daily_spend_key, 86400)
        await pipe.execute()
        spent = await self.redis.get(self.daily_spend_key)
        spent_float = float(spent) if spent else 0.0
        logger.info("api_spend_recorded", provider=provider, cost=cost, total_spent=spent_float)
        if spent_float >= self.budget * 0.9:
            logger.critical("budget_critical_90", spent=spent_float, budget=self.budget)
        await self.redis.zincrby("budget:by_provider", cost, provider)

class BudgetExceededError(Exception):
    pass
