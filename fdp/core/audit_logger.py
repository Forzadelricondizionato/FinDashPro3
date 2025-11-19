import json
import asyncpg
import structlog
from datetime import datetime
from typing import Dict, Any

logger = structlog.get_logger()

class AuditLogger:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
    
    async def log(self, event_type: str, user_id: str, ticker: str, action: str, details: Dict[str, Any]):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO audit_log (event_type, user_id, ticker, action, details) VALUES ($1, $2, $3, $4, $5)",
                event_type, user_id, ticker, action, json.dumps(details)
            )

