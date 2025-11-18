"""Test rate limiter and circuit breaker."""
import pytest
import asyncio
from unittest.mock import Mock, patch
from fdp.core.rate_limiter import BudgetExceededError
from fdp.core.circuit_breaker import CircuitOpenError

@pytest.mark.asyncio
async def test_rate_limiter_acquire_success(rate_limiter, mock_redis):
    """Test successful rate limit acquire."""
    await rate_limiter.acquire("yahoo", "test_key", 1000)
    # Should not raise

@pytest.mark.asyncio
async def test_rate_limiter_budget_exceeded(rate_limiter, mock_redis):
    """Test budget exceeded."""
    # Pre-fill budget
    await mock_redis.set("budget:daily_spent", "5.0")
    
    with pytest.raises(BudgetExceededError):
        await rate_limiter.acquire("yahoo", "test_key", 1000)

@pytest.mark.asyncio
async def test_rate_limiter_atomic_check(rate_limiter, mock_redis):
    """Test atomic budget check."""
    # Should pass with budget available
    assert await rate_limiter._check_budget_atomic() is True
    
    # Fill budget to limit
    await mock_redis.set("budget:daily_spent", "4.99")
    assert await rate_limiter._check_budget_atomic() is True
    
    # Exceed budget
    await mock_redis.set("budget:daily_spent", "5.1")
    assert await rate_limiter._check_budget_atomic() is False

@pytest.mark.asyncio
async def test_circuit_breaker_closed(circuit_breaker):
    """Test circuit breaker closed state."""
    async def success_func():
        return "success"
    
    result = await circuit_breaker.call("test_provider", success_func)
    assert result == "success"
    
    # Check metrics
    metrics = circuit_breaker.get_metrics("test_provider")
    assert metrics["state"] == "closed"

@pytest.mark.asyncio
async def test_circuit_breaker_open_after_failures(circuit_breaker):
    """Test circuit breaker opens after failures."""
    async def failure_func():
        raise Exception("Simulated failure")
    
    # Trigger failures
    for _ in range(3):
        try:
            await circuit_breaker.call("test_provider", failure_func)
        except:
            pass
    
    # Should be open now
    metrics = circuit_breaker.get_metrics("test_provider")
    assert metrics["state"] == "open"
    
    # Should raise CircuitOpenError
    with pytest.raises(CircuitOpenError):
        await circuit_breaker.call("test_provider", lambda: "test")

@pytest.mark.asyncio
async def test_circuit_breaker_half_open_recovery(circuit_breaker):
    """Test circuit breaker half-open recovery."""
    # Force open state
    circuit_breaker.states["test_provider"] = "open"
    circuit_breaker.last_failure["test_provider"] = time.time() - 301
    
    async def success_func():
        return "recovered"
    
    # Should transition to half-open and succeed
    result = await circuit_breaker.call("test_provider", success_func)
    assert result == "recovered"
    
    # Should be half-open after first success
    metrics = circuit_breaker.get_metrics("test_provider")
    assert metrics["state"] == "half-open"

@pytest.mark.asyncio
async def test_rate_limiter_provider_lock_cleanup(rate_limiter, mock_redis):
    """Test rate limiter lock cleanup."""
    # Fill locks beyond threshold
    for i in range(1001):
        await rate_limiter.acquire(f"provider_{i}", f"key_{i}", 100)
    
    # Should trigger cleanup
    assert len(rate_limiter._locks) < 1000
