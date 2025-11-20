# tests/unit/test_circuit_breaker.py
import pytest
import asyncio
from unittest.mock import AsyncMock
from fdp.core.circuit_breaker import CircuitBreaker, CircuitOpenError

pytestmark = pytest.mark.asyncio

class TestCircuitBreaker:
    async def test_successful_call(self):
        cb = CircuitBreaker(failure_threshold=3)
        mock_func = AsyncMock(return_value="success")
        result = await cb.call("test_provider", mock_func)
        assert result == "success"
        assert cb.states["test_provider"] == "closed"
        assert cb.failures["test_provider"] == 0
    
    async def test_circuit_open_after_failures(self):
        cb = CircuitBreaker(failure_threshold=3)
        mock_func = AsyncMock(side_effect=Exception("API Error"))
        
        for _ in range(3):
            with pytest.raises(Exception):
                await cb.call("test_provider", mock_func)
        
        assert cb.states["test_provider"] == "open"
        
        with pytest.raises(CircuitOpenError):
            await cb.call("test_provider", mock_func)
    
    async def test_half_open_recovery(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1)
        mock_func_fail = AsyncMock(side_effect=Exception("API Error"))
        
        for _ in range(2):
            with pytest.raises(Exception):
                await cb.call("test_provider", mock_func_fail)
        
        assert cb.states["test_provider"] == "open"
        await asyncio.sleep(1.1)
        
        mock_func_success = AsyncMock(return_value="recovered")
        result = await cb.call("test_provider", mock_func_success)
        assert result == "recovered"
        assert cb.states["test_provider"] == "half-open"
    
    async def test_recovery_after_half_open_successes(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1)
        
        for _ in range(2):
            with pytest.raises(Exception):
                await cb.call("test_provider", AsyncMock(side_effect=Exception()))
        
        await asyncio.sleep(1.1)
        mock_success = AsyncMock(return_value="ok")
        await cb.call("test_provider", mock_success)
        await cb.call("test_provider", mock_success)
        
        assert cb.states["test_provider"] == "closed"
        assert cb.failures["test_provider"] == 0
    
    async def test_metrics_tracking(self):
        cb = CircuitBreaker(failure_threshold=3)
        metrics = cb.get_metrics("test_provider")
        assert metrics["state"] == "closed"
        assert metrics["failures"] == 0
        
        with pytest.raises(Exception):
            await cb.call("test_provider", AsyncMock(side_effect=Exception()))
        
        metrics = cb.get_metrics("test_provider")
        assert metrics["failures"] == 1
        assert "last_failure" in metrics
