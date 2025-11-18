import asyncio
import time
from collections import defaultdict
import redis.asyncio as redis
import structlog

logger = structlog.get_logger()

class TokenBucketRateLimiter:
    def __init__(self, redis_client: redis.Redis, budget: float = 5.0):
        self.redis = redis_client
        self.budget = budget
        self.daily_spend_key = "budget:daily_spent"
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._lock_cleanup_threshold = 1000
    
    async def acquire(self, provider: str, key: str, limit_per_min: int):
        # Atomic budget check
        budget_ok = await self._check_budget_atomic()
        if not budget_ok:
            raise BudgetExceededError(f"Daily budget exceeded: {self.budget}€")
        
        bucket_key = f"rate_limit:{provider}:{key}"
        
        async with self._locks[provider]:
            # Periodic lock cleanup
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
            new_tokens = min(limit_per_min, tokens + elapsed * limit_per_min)
            
            if new_tokens < 1.0:
                wait_time = (1.0 - new_tokens) / limit_per_min * 60
                logger.warning("rate_limit_wait", provider=provider, wait=wait_time)
                await asyncio.sleep(wait_time)
                now = time.time()
                elapsed = (now - last_refill) / 60.0
                new_tokens = min(1.0, elapsed * limit_per_min)
            
            pipe = self.redis.pipeline()
            pipe.hset(bucket_key, mapping={"tokens": new_tokens - 1, "last_refill": now})
            pipe.expire(bucket_key, 3600)
            await pipe.execute()
    
    async def _check_budget_atomic(self) -> bool:
        """Atomic budget check with Lua script."""
        script = """
        local spent = redis.call('GET', KEYS[1]) or '0'
        local budget = tonumber(ARGV[1])
        return tonumber(spent) < budget
        """
        return bool(await self.redis.eval(script, 1, self.daily_spend_key, self.budget))
    
    async def record_spend(self, provider: str, cost: float):
        """Record API spend atomically."""
        pipe = self.redis.pipeline()
        pipe.incrbyfloat(self.daily_spend_key, cost)
        pipe.expire(self.daily_spend_key, 86400)
        await pipe.execute()
        
        spent = await self.redis.get(self.daily_spend_key)
        spent_float = float(spent) if spent else 0.0
        logger.info("api_spend_recorded", provider=provider, cost=cost, total_spent=spent_float)
        
        if spent_float >= self.budget * 0.9:
            logger.critical("budget_critical_90", spent=spent_float, budget=self.budget)
        
        # Track per-provider costs
        await self.redis.zincrby("budget:by_provider", cost, provider)

class BudgetExceededError(Exception):
    pass
