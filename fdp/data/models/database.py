import asyncpg
from typing import Optional
import structlog

logger = structlog.get_logger()

class DatabaseManager:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def insert_signal(self, ticker: str, action: str, confidence: float, predicted_return: float):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO signals (ticker, action, confidence, predicted_return, timestamp) VALUES ($1, $2, $3, $4, NOW())",
                ticker, action, confidence, predicted_return
            )

    async def insert_order(self, order_id: str, ticker: str, action: str, quantity: float, price: Optional[float]):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO orders (order_id, ticker, action, quantity, price, status) VALUES ($1, $2, $3, $4, $5, 'submitted')",
                order_id, ticker, action, quantity, price
            )

    async def update_order_status(self, order_id: str, status: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE orders SET status = $1, updated_at = NOW() WHERE order_id = $2",
                status, order_id
            )

    async def get_portfolio_value(self) -> float:
        async with self.pool.acquire() as conn:
            result = await conn.fetchval("SELECT SUM(quantity * price) FROM orders WHERE status = 'filled'")
            return result or 0.0

    async def get_daily_pnl(self) -> float:
        async with self.pool.acquire() as conn:
            today = "CURRENT_DATE"
            result = await conn.fetchval(f"SELECT SUM(profit) FROM trades WHERE DATE(timestamp) = {today}")
            return result or 0.0

    async def insert_ml_metrics(self, ticker: str, metric_name: str, metric_value: float):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO ml_metrics (ticker, metric_name, metric_value) VALUES ($1, $2, $3)",
                ticker, metric_name, metric_value
            )

    async def log_audit(self, event_type: str, event_data: dict, user_id: str = "system"):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO audit_log (event_type, event_data, user_id, origin_ip) VALUES ($1, $2, $3, '127.0.0.1')",
                event_type, json.dumps(event_data), user_id
            )
