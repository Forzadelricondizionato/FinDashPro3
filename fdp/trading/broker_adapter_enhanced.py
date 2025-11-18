import asyncio
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import asyncpg
import structlog
from fdp.core.config import config

logger = structlog.get_logger()

class EnhancedOrder:
    """Enhanced order with metadata and risk checks."""
    
    def __init__(self, symbol: str, action: str, quantity: float, 
                 order_type: str, limit_price: float = 0.0, 
                 idempotency_key: str = "", metadata: Optional[Dict] = None):
        self.symbol = symbol
        self.action = action
        self.quantity = quantity
        self.order_type = order_type
        self.limit_price = limit_price
        self.idempotency_key = idempotency_key
        self.metadata = metadata or {}
        self.status = "created"
        self.order_id = None

class BrokerAdapter(ABC):
    """Abstract broker adapter."""
    
    @abstractmethod
    async def get_account_summary(self) -> Dict[str, float]:
        pass
    
    @abstractmethod
    async def place_order(self, order: EnhancedOrder) -> str:
        pass
    
    @abstractmethod
    async def sync_orders(self):
        pass
    
    @abstractmethod
    async def get_positions(self) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def graceful_shutdown(self):
        pass

class PaperBrokerAdapter(BrokerAdapter):
    """Paper trading adapter."""
    
    def __init__(self, config: Any, notifier: Any, redis: Any):
        self.config = config
        self.notifier = notifier
        self.redis = redis
        self.db_pool: Optional[asyncpg.Pool] = None
        self.positions: Dict[str, Dict] = {}
        self.orders: Dict[str, EnhancedOrder] = {}
        self.initial_capital = getattr(config, 'paper_trading_capital', 100000)
        self.cash = self.initial_capital
        self.running = True
    
    async def init_db(self):
        """Initialize database pool."""
        if self.db_pool is None:
            self.db_pool = await asyncpg.create_pool(
                config.database_url, 
                min_size=2, 
                max_size=10
            )
            logger.info("paper_broker_db_connected")
    
    async def get_account_summary(self) -> Dict[str, float]:
        """Get paper account summary."""
        return {
            "cash": self.cash,
            "portfolio_value": self.cash + sum(pos.get('market_value', 0) for pos in self.positions.values()),
            "initial_capital": self.initial_capital
        }
    
    async def place_order(self, order: EnhancedOrder) -> str:
        """Place paper order."""
        try:
            # Idempotency check
            if order.idempotency_key and await self.redis.exists(order.idempotency_key):
                logger.info("paper_order_duplicate", order_id=order.order_id)
                return "duplicate_suppressed"
            
            # Calculate cost
            price = order.limit_price or 100.0  # Mock price if not provided
            cost = order.quantity * price
            
            # Risk check
            if order.action == "buy" and cost > self.cash:
                raise Exception("Insufficient funds")
            
            # Execute
            order.order_id = f"paper_{order.symbol}_{int(asyncio.get_event_loop().time())}"
            order.status = "filled"
            
            if order.action == "buy":
                self.cash -= cost
                self.positions[order.symbol] = {
                    "quantity": order.quantity,
                    "entry_price": price,
                    "market_value": cost
                }
            else:  # sell
                if order.symbol in self.positions:
                    del self.positions[order.symbol]
                self.cash += cost
            
            # Store order
            self.orders[order.order_id] = order
            
            # Audit
            await self._log_paper_trade(order)
            
            logger.critical("paper_order_executed", order_id=order.order_id, symbol=order.symbol)
            return order.order_id
            
        except Exception as e:
            logger.error("paper_order_failed", error=str(e))
            raise
    
    async def sync_orders(self):
        """Sync paper orders (no-op)."""
        await asyncio.sleep(0.1)
    
    async def get_positions(self) -> Dict[str, Any]:
        """Get open positions."""
        return self.positions
    
    async def graceful_shutdown(self):
        """Graceful shutdown."""
        self.running = False
        if self.db_pool:
            await self.db_pool.close()
        logger.info("paper_broker_shutdown")
    
    async def _log_paper_trade(self, order: EnhancedOrder):
        """Log paper trade to database."""
        if self.db_pool:
            await self.db_pool.execute(
                """
                INSERT INTO orders (order_id, ticker, action, quantity, price, status, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (order_id) DO NOTHING
                """,
                order.order_id,
                order.symbol,
                order.action,
                order.quantity,
                order.limit_price,
                order.status,
                json.dumps(order.metadata)
            )

class AlpacaBrokerAdapter(BrokerAdapter):
    """Alpaca broker adapter."""
    
    def __init__(self, config: Any, notifier: Any, redis: Any):
        self.config = config
        self.notifier = notifier
        self.redis = redis
        self.session: Optional[aiohttp.ClientSession] = None
        self.base_url = "https://paper-api.alpaca.markets" if config.alpaca_paper else "https://api.alpaca.markets"
    
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_account_summary(self) -> Dict[str, float]:
        """Get Alpaca account summary."""
        url = f"{self.base_url}/v2/account"
        headers = {
            "APCA-API-KEY-ID": self.config.alpaca_key,
            "APCA-API-SECRET-KEY": self.config.alpaca_secret
        }
        
        async with self.session.get(url, headers=headers) as resp:
            if resp.status != 200:
                raise Exception(f"Alpaca API error: {resp.status}")
            
            data = await resp.json()
            return {
                "cash": float(data.get("cash", 0)),
                "portfolio_value": float(data.get("portfolio_value", 0)),
                "initial_capital": getattr(self.config, 'paper_trading_capital', 100000)
            }
    
    async def place_order(self, order: EnhancedOrder) -> str:
        """Place Alpaca order."""
        url = f"{self.base_url}/v2/orders"
        headers = {
            "APCA-API-KEY-ID": self.config.alpaca_key,
            "APCA-API-SECRET-KEY": self.config.alpaca_secret,
            "Content-Type": "application/json"
        }
        
        payload = {
            "symbol": order.symbol,
            "qty": str(order.quantity),  # Alpaca expects string
            "side": order.action,
            "type": order.order_type,
            "time_in_force": "gtc",
            "client_order_id": order.idempotency_key
        }
        
        if order.limit_price > 0:
            payload["limit_price"] = str(order.limit_price)
        
        async with self.session.post(url, headers=headers, json=payload) as resp:
            if resp.status not in [200, 201]:
                raise Exception(f"Order failed: {resp.status} {await resp.text()}")
            
            data = await resp.json()
            return data.get("id")
    
    async def sync_orders(self):
        """Sync orders from Alpaca."""
        url = f"{self.base_url}/v2/orders"
        headers = {
            "APCA-API-KEY-ID": self.config.alpaca_key,
            "APCA-API-SECRET-KEY": self.config.alpaca_secret
        }
        
        async with self.session.get(url, headers=headers) as resp:
            if resp.status == 200:
                orders = await resp.json()
                for order in orders:
                    await self.redis.setex(
                        f"order:status:{order['id']}",
                        3600,
                        json.dumps(order)
                    )
    
    async def get_positions(self) -> Dict[str, Any]:
        """Get Alpaca positions."""
        url = f"{self.base_url}/v2/positions"
        headers = {
            "APCA-API-KEY-ID": self.config.alpaca_key,
            "APCA-API-SECRET-KEY": self.config.alpaca_secret
        }
        
        async with self.session.get(url, headers=headers) as resp:
            if resp.status != 200:
                return {}
            
            positions = await resp.json()
            return {pos["symbol"]: pos for pos in positions}
    
    async def graceful_shutdown(self):
        """Graceful shutdown."""
        if self.session:
            await self.session.close()
        logger.info("alpaca_broker_shutdown")

def get_broker_adapter(config: Any, notifier: Any, redis: Any) -> BrokerAdapter:
    """Factory function to get broker adapter."""
    if config.execution_mode == "paper":
        adapter = PaperBrokerAdapter(config, notifier, redis)
        asyncio.create_task(adapter.init_db())
        return adapter
    elif config.execution_mode == "alpaca":
        return AlpacaBrokerAdapter(config, notifier, redis)
    elif config.execution_mode == "ibkr":
        # IBKR adapter would go here
        raise NotImplementedError("IBKR adapter not implemented")
    else:
        # Default to paper for safety
        return PaperBrokerAdapter(config, notifier, redis)
