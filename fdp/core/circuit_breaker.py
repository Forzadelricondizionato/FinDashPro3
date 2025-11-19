import time
from collections import defaultdict
from typing import Callable, Any, Dict
import structlog
import numpy as np
from scipy.stats import expon

logger = structlog.get_logger()

class MLCircuitBreaker:
    def __init__(self, failure_threshold: int = 3, base_recovery_timeout: int = 300):
        self.failure_threshold = failure_threshold
        self.base_recovery_timeout = base_recovery_timeout
        self.failures: defaultdict[str, int] = defaultdict(int)
        self.last_failure: defaultdict[str, float] = defaultdict(float)
        self.states: defaultdict[str, str] = defaultdict(lambda: "closed")
        self.success_half_open: defaultdict[str, int] = defaultdict(int)
        self.failure_times: defaultdict[str, List[float]] = defaultdict(list)
        self.metrics: Dict[str, Dict] = {}

    async def call(self, provider: str, func: Callable, *args, **kwargs) -> Any:
        state = self.states[provider]
        now = time.time()
        if state == "open":
            adaptive_timeout = self._calculate_ml_recovery_timeout(provider)
            if now - self.last_failure[provider] > adaptive_timeout:
                self.states[provider] = "half-open"
                self.success_half_open[provider] = 0
                logger.info("circuit_half_open_ml", provider=provider, timeout=adaptive_timeout)
            else:
                wait = adaptive_timeout - (now - self.last_failure[provider])
                raise CircuitOpenError(f"Circuit open for {provider}, wait {wait:.0f}s", provider=provider)
        try:
            result = await func(*args, **kwargs)
            if state == "half-open":
                self.success_half_open[provider] += 1
                if self.success_half_open[provider] >= 2:
                    self.states[provider] = "closed"
                    self.failures[provider] = 0
                    self.failure_times[provider].clear()
                    logger.info("circuit_closed_ml", provider=provider)
            self.failures[provider] = 0
            return result
        except Exception as e:
            self.failures[provider] += 1
            self.last_failure[provider] = now
            self.failure_times[provider].append(now)
            if self.failures[provider] >= self.failure_threshold:
                self.states[provider] = "open"
                logger.critical("circuit_opened_ml", provider=provider, failures=self.failures[provider])
            raise

    def _calculate_ml_recovery_timeout(self, provider: str) -> float:
        if not self.failure_times[provider]:
            return self.base_recovery_timeout
        intervals = np.diff(self.failure_times[provider])
        if len(intervals) < 2:
            return self.base_recovery_timeout
        lambda_est = 1 / np.mean(intervals)
        predicted_next_fail = expon.rvs(scale=1/lambda_est)
        adaptive_timeout = max(60, min(600, self.base_recovery_timeout * (1 + predicted_next_fail / 100)))
        logger.debug("ml_recovery_calculated", provider=provider, timeout=adaptive_timeout)
        return adaptive_timeout

    def get_metrics(self, provider: str) -> Dict:
        return {
            "state": self.states[provider],
            "failures": self.failures[provider],
            "last_failure": self.last_failure[provider],
            "recovery_timeout_ml": self._calculate_ml_recovery_timeout(provider)
        }

class CircuitOpenError(Exception):
    def __init__(self, message: str, provider: str = None):
        self.provider = provider
        super().__init__(message)
