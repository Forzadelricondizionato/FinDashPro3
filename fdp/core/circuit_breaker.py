import time
from collections import defaultdict
from typing import Callable, Any, Dict
import structlog

logger = structlog.get_logger()

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, recovery_timeout: int = 300):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        
        # State management
        self.failures: defaultdict[str, int] = defaultdict(int)
        self.last_failure: defaultdict[str, float] = defaultdict(float)
        self.states: defaultdict[str, str] = defaultdict(lambda: "closed")
        self.success_half_open: defaultdict[str, int] = defaultdict(int)
        
        # Metrics
        self.metrics: Dict[str, Dict] = {}
    
    async def call(self, provider: str, func: Callable, *args, **kwargs) -> Any:
        state = self.states[provider]
        now = time.time()
        
        if state == "open":
            if now - self.last_failure[provider] > self.recovery_timeout:
                self.states[provider] = "half-open"
                self.success_half_open[provider] = 0
                logger.info("circuit_half_open", provider=provider)
            else:
                wait = self.recovery_timeout - (now - self.last_failure[provider])
                raise CircuitOpenError(f"Circuit open for {provider}, wait {wait:.0f}s", provider=provider)
        
        try:
            result = await func(*args, **kwargs)
            
            # Success in half-open state
            if state == "half-open":
                self.success_half_open[provider] += 1
                if self.success_half_open[provider] >= 2:
                    self.states[provider] = "closed"
                    self.failures[provider] = 0
                    logger.info("circuit_closed", provider=provider)
            
            # Reset failures on success
            self.failures[provider] = 0
            return result
            
        except Exception as e:
            self.failures[provider] += 1
            self.last_failure[provider] = now
            
            if self.failures[provider] >= self.failure_threshold:
                self.states[provider] = "open"
                logger.critical("circuit_opened", provider=provider, failures=self.failures[provider])
            
            raise
    
    def get_metrics(self, provider: str) -> Dict:
        """Get circuit breaker metrics for a provider."""
        return {
            "state": self.states[provider],
            "failures": self.failures[provider],
            "last_failure": self.last_failure[provider],
            "recovery_timeout": self.recovery_timeout
        }

class CircuitOpenError(Exception):
    def __init__(self, message: str, provider: str = None):
        self.provider = provider
        super().__init__(message)
