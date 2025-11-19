from fastapi import APIRouter
from pydantic import BaseModel
from typing import Dict, Any
import psutil
import time

router = APIRouter()

class HealthCheck(BaseModel):
    status: str
    checks: Dict[str, Any]
    uptime: float

startup_time = time.time()

@router.get("/health", response_model=HealthCheck)
async def health_check():
    checks = {
        "cpu": psutil.cpu_percent(),
        "memory": psutil.virtual_memory()._asdict(),
        "disk": psutil.disk_usage('/')._asdict(),
    }
    uptime = time.time() - startup_time
    
    try:
        from main import orchestrator
        redis_ping = await orchestrator.redis.ping()
        checks["redis"] = "connected" if redis_ping else "disconnected"
    except:
        checks["redis"] = "error"
    
    try:
        db_status = orchestrator.db_pool is not None
        checks["database"] = "connected" if db_status else "disconnected"
    except:
        checks["database"] = "error"
    
    status = "healthy" if all(v in ["connected", "ok"] for v in checks.values()) else "degraded"
    
    return HealthCheck(status=status, checks=checks, uptime=uptime)

