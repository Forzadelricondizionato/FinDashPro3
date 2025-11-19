import asyncio
import json
from datetime import datetime
from typing import Dict, Any, Optional
import redis.asyncio as redis
import structlog

logger = structlog.get_logger()

class EnhancedOrder:
    def __init__(self, symbol: str, action: str, quantity: float, order_type: str = "market",
                 limit_price: Optional[float] = None, stop_price: Optional[float] = None,
                 idempotency_key: Optional[str] = None, metadata: Optional[Dict] = None):
        self.symbol = symbol
        self.action = action
        self.quantity = quantity
        self.order_type = order_type
        self.limit_price = limit_price
        self.stop_price = stop_price
        self.idempotency_key = idempotency_key
        self.metadata = metadata or {}
        self.order_id = None
        self.status = "pending"
        self.created_at = datetime.now()

class BaseBrokerAdapter:
    def __init__(self, config, notifier, redis_client: redis.Redis):
        self.config = config
        self.notifier = notifier
        self.redis = redis_client
        self.connected = False

    async def connect(self):
        self.connected = True
        logger.info("broker_connected", adapter=self.__class__.__name__)

    async def disconnect(self):
        self.connected = False
        logger.info("broker_disconnected")

    async def get_account_summary(self) -> Dict[str, float]:
        return {
            "total_cash": self.config.paper_trading_capital,
            "available_cash": self.config.paper_trading_capital * 0.95,
            "portfolio_value": self.config.paper_trading_capital,
            "volatility_30d": 0.02
        }

    async def place_order(self, order: EnhancedOrder) -> str:
        if not self.connected:
            await self.connect()
        order_id = f"{order.symbol}_{int(datetime.now().timestamp())}_{np.random.randint(1000)}"
        order.order_id = order_id
        order.status = "submitted"
        if self.config.execution_mode == "paper":
            await self._simulate_fill(order)
        await self.redis.hset(f"order:{order_id}", mapping={
            "symbol": order.symbol,
            "action": order.action,
            "quantity": order.quantity,
            "status": order.status,
            "idempotency_key": order.idempotency_key or ""
        })
        await self.redis.publish("orders:status", json.dumps({
            "order_id": order_id,
            "status": order.status,
            "symbol": order.symbol
        }))
        logger.critical("order_placed", order_id=order_id, symbol=order.symbol, action=order.action)
        return order_id

    async def _simulate_fill(self, order: EnhancedOrder):
        await asyncio.sleep(2)
        order.status = "filled"
        fill_price = order.limit_price or np.random.uniform(95, 105)
        await self.redis.hset(f"order:{order.order_id}", "status", "filled")
        await self.redis.hset(f"order:{order.order_id}", "fill_price", fill_price)
        logger.info("order_filled_simulated", order_id=order.order_id, symbol=order.symbol, price=fill_price)

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        data = await self.redis.hgetall(f"order:{order_id}")
        if not data:
            return {"status": "not_found"}
        return {
            "order_id": order_id,
            "symbol": data.get(b'symbol', b'').decode() or data.get('symbol', ''),
            "status": data.get(b'status', b'').decode() or data.get('status', 'unknown'),
            "fill_price": float(data.get(b'fill_price', 0)) if data.get(b'fill_price') else None
        }

    async def cancel_order(self, order_id: str) -> bool:
        await self.redis.hset(f"order:{order_id}", "status", "cancelled")
        logger.info("order_cancelled", order_id=order_id)
        return True

    async def sync_orders(self):
        order_keys = await self.redis.keys("order:*")
        for key in order_keys:
            status = await self.redis.hget(key, "status")
            if status in ["submitted", "pending"]:
                await asyncio.sleep(0.1)

    async def graceful_shutdown(self):
        await self.disconnect()
        logger.info("broker_shutdown_complete")

def get_broker_adapter(config, notifier, redis_client: redis.Redis):
    if config.execution_mode == "ibkr":
        return IBKRBrockerAdapter(config, notifier, redis_client)
    elif config.execution_mode == "alpaca":
        return AlpacaBrokerAdapter(config, notifier, redis_client)
    else:
        return PaperBrokerAdapter(config, notifier, redis_client)

class PaperBrokerAdapter(BaseBrokerAdapter):
    async def _simulate_fill(self, order: EnhancedOrder):
        await asyncio.sleep(1)
        order.status = "filled"
        fill_price = order.limit_price or np.random.uniform(98, 102)
        await self.redis.hset(f"order:{order.order_id}", "status", "filled")
        await self.redis.hset(f"order:{order.order_id}", "fill_price", fill_price)
        await self.redis.hset(f"order:{order.order_id}", "filled_at", datetime.now().isoformat())
        logger.info("paper_order_filled", order_id=order.order_id, symbol=order.symbol, price=fill_price)

class IBKRBrockerAdapter(BaseBrokerAdapter):
    def __init__(self, config, notifier, redis_client: redis.Redis):
        super().__init__(config, notifier, redis_client)
        self.host = config.ibkr_host
        self.port = config.ibkr_port
        self.client_id = config.ibkr_client_id

    async def connect(self):
        try:
            from ib_insync import IB, Stock
            self.ib = IB()
            await self.ib.connectAsync(self.host, self.port, self.client_id)
            self.connected = True
            logger.info("ibkr_connected", host=self.host, port=self.port)
        except Exception as e:
            logger.error("ibkr_connect_failed", error=str(e))
            self.connected = False

    async def place_order(self, order: EnhancedOrder) -> str:
        if not self.connected:
            await self.connect()
        if not self.connected:
            raise ConnectionError("IBKR not connected")
        from ib_insync import Stock, MarketOrder, LimitOrder
        contract = Stock(order.symbol, 'SMART', 'USD')
        ib_order = LimitOrder(order.action, order.quantity, order.limit_price) if order.limit_price else MarketOrder(order.action, order.quantity)
        trade = self.ib.placeOrder(contract, ib_order)
        logger.info("ibkr_order_submitted", order_id=trade.order.orderId, symbol=order.symbol)
        return str(trade.order.orderId)

class AlpacaBrokerAdapter(BaseBrokerAdapter):
    def __init__(self, config, notifier, redis_client: redis.Redis):
        super().__init__(config, notifier, redis_client)
        self.api_key = config.alpaca_key
        self.secret = config.alpaca_secret
        self.base_url = "https://paper-api.alpaca.markets" if config.alpaca_paper else "https://api.alpaca.markets"

    async def connect(self):
        try:
            headers = {"APCA-API-KEY-ID": self.api_key, "APCA-API-SECRET-KEY": self.secret}
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}/v2/account", headers=headers) as resp:
                    if resp.status == 200:
                        self.connected = True
                        logger.info("alpaca_connected")
                    else:
                        raise ConnectionError("Alpaca auth failed")
        except Exception as e:
            logger.error("alpaca_connect_failed", error=str(e))
            self.connected = False

    async def place_order(self, order: EnhancedOrder) -> str:
        if not self.connected:
            await self.connect()
        headers = {"APCA-API-KEY-ID": self.api_key, "APCA-API-SECRET-KEY": self.secret}
        payload = {
            "symbol": order.symbol,
            "qty": order.quantity,
            "side": order.action,
            "type": order.order_type,
            "time_in_force": "day"
        }
        if order.limit_price:
            payload["limit_price"] = order.limit_price
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.base_url}/v2/orders", headers=headers, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    order_id = data.get("id")
                    logger.info("alpaca_order_submitted", order_id=order_id, symbol=order.symbol)
                    return order_id
                else:
                    raise Exception(f"Alpaca order failed: {resp.status}")
