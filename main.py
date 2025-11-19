import asyncio
import signal
import sys
from pathlib import Path
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import uvicorn
import structlog
from fdp.core.orchestrator import FinDashProOrchestrator
from fdp.core.config import Config
from fdp.core.vault_client import VaultClient
from fdp.api.routes import router

logger = structlog.get_logger()
app = FastAPI(title="FinDashPro API", version="3.2.0")
security = HTTPBearer()

class HealthResponse(BaseModel):
    status: str
    version: str
    services: dict

@app.on_event("startup")
async def startup_event():
    global orchestrator
    orchestrator = FinDashProOrchestrator()
    await orchestrator.init()
    logger.info("API started")

@app.on_event("shutdown")
async def shutdown_event():
    await orchestrator.shutdown()
    logger.info("API shutdown")

@app.get("/health", response_model=HealthResponse)
async def health_check():
    redis_status = await orchestrator.redis.ping()
    db_status = orchestrator.db_pool is not None
    vault_status = VaultClient().is_initialized()
    return HealthResponse(
        status="healthy" if all([redis_status, db_status, vault_status]) else "degraded",
        version="3.2.0",
        services={
            "redis": redis_status,
            "postgres": db_status,
            "vault": vault_status
        }
    )

@app.post("/kill-switch")
async def kill_switch(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != orchestrator.config.kill_switch_token:
        raise HTTPException(status_code=403, detail="Invalid token")
    orchestrator.running = False
    logger.critical("Kill switch activated")
    return {"status": "killed"}

@app.get("/metrics/prometheus")
async def prometheus_metrics():
    from prometheus_client import generate_latest
    return generate_latest()

app.include_router(router, prefix="/api/v1")

def main():
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False, workers=1)

if __name__ == "__main__":
    main()
