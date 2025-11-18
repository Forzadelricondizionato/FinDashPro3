import pytest
import asyncio
from unittest.mock import AsyncMock
from fdp.core.circuit_breaker import CircuitBreaker, CircuitOpenError

pytestmark = pytest.mark.asyncio

class TestCircuitBreaker:
    async def test_successful_call(self):
        """Test successful API call."""
        cb = CircuitBreaker(failure_threshold=3)
        mock_func = AsyncMock(return_value="success")
        
        result = await cb.call("test_provider", mock_func)
        
        assert result == "success"
        assert cb.states["test_provider"] == "closed"
        assert cb.failures["test_provider"] == 0
    
    async def test_circuit_open_after_failures(self):
        """Test circuit opens after threshold failures."""
        cb = CircuitBreaker(failure_threshold=3)
        mock_func = AsyncMock(side_effect=Exception("API Error"))
        
        # First 3 calls should raise but not open circuit
        for _ in range(3):
            with pytest.raises(Exception):
                await cb.call("test_provider", mock_func)
        
        assert cb.states["test_provider"] == "open"
        
        # 4th call should raise CircuitOpenError immediately
        with pytest.raises(CircuitOpenError):
            await cb.call("test_provider", mock_func)
    
    async def test_half_open_recovery(self):
        """Test half-open state and recovery."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1)
        
        # Open circuit
        mock_func_fail = AsyncMock(side_effect=Exception("API Error"))
        for _ in range(2):
            with pytest.raises(Exception):
                await cb.call("test_provider", mock_func_fail)
        
        assert cb.states["test_provider"] == "open"
        
        # Wait for recovery timeout
        await asyncio.sleep(1.1)
        
        # Circuit should be half-open
        mock_func_success = AsyncMock(return_value="recovered")
        result = await cb.call("test_provider", mock_func_success)
        
        assert result == "recovered"
        assert cb.states["test_provider"] == "half-open"
        assert cb.success_half_open["test_provider"] == 1
    
    async def test_recovery_after_half_open_successes(self):
        """Test circuit closes after successful half-open calls."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1)
        
        # Open circuit
        for _ in range(2):
            with pytest.raises(Exception):
                await cb.call("test_provider", AsyncMock(side_effect=Exception()))
        
        # Wait and recover
        await asyncio.sleep(1.1)
        
        # Two successful calls should close circuit
        mock_success = AsyncMock(return_value="ok")
        await cb.call("test_provider", mock_success)
        await cb.call("test_provider", mock_success)
        
        assert cb.states["test_provider"] == "closed"
        assert cb.failures["test_provider"] == 0
    
    async def test_metrics_tracking(self):
        """Test circuit breaker metrics."""
        cb = CircuitBreaker(failure_threshold=3)
        
        metrics = cb.get_metrics("test_provider")
        assert metrics["state"] == "closed"
        assert metrics["failures"] == 0
        
        # After failure
        with pytest.raises(Exception):
            await cb.call("test_provider", AsyncMock(side_effect=Exception()))
        
        metrics = cb.get_metrics("test_provider")
        assert metrics["failures"] == 1
        assert "last_failure" in metrics
