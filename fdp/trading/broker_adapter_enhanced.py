import asyncio
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from dataclasses import dataclass
from pydantic import BaseModel
import ib_insync as ib
import structlog

logger = structlog.get_logger()

class EnhancedOrder(BaseModel):
    symbol: str
    action: str
    quantity: int
    order_type: str
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    idempotency_key: Optional[str] = None
    time_in_force: str = "GTC"
    status: str = "pending"
    executed_price: Optional[float] = None
    executed_quantity: int = 0

class BaseBrokerAdapter(ABC):
    @abstractmethod
    async def connect(self):
        pass
    
    @abstractmethod
    async def disconnect(self):
        pass
    
    @abstractmethod
    async def get_account_summary(self) -> Dict[str, float]:
        pass
    
    @abstractmethod
    async def place_order(self, order: EnhancedOrder) -> str:
        pass
    
    @abstractmethod
    async def get_positions(self) -> Dict[str, Dict[str, Any]]:
        pass
    
    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        pass

class IBKRBrokerAdapter(BaseBrokerAdapter):
    def __init__(self, config):
        self.config = config
        self.ib = ib.IB()
        self.connected = False
    
    async def connect(self):
        if self.connected:
            return
        await asyncio.get_event_loop().run_in_executor(None, lambda: self.ib.connect(
            self.config.ibkr_host, self.config.ibkr_port, self.config.ibkr_client_id
        ))
        self.connected = True
        logger.info("IBKR connected")
    
    async def disconnect(self):
        if self.connected:
            self.ib.disconnect()
            self.connected = False
    
    async def get_account_summary(self) -> Dict[str, float]:
        await self.connect()
        account = self.ib.accountSummary()
        cash = sum(float(item.value) for item in account if item.tag == "TotalCashBalance" and item.currency == "BASE")
        portfolio_value = sum(float(item.marketValue) for item in self.ib.portfolio())
        return {"cash": cash, "portfolio_value": portfolio_value}
    
    async def place_order(self, order: EnhancedOrder) -> str:
        await self.connect()
        contract = ib.Stock(order.symbol, "SMART", "USD")
        ib_order = ib.Order()
        ib_order.action = order.action
        ib_order.totalQuantity = order.quantity
        ib_order.orderType = order.order_type
        if order.limit_price:
            ib_order.lmtPrice = order.limit_price
        
        trade = self.ib.placeOrder(contract, ib_order)
        return str(trade.order.orderId)
    
    async def get_positions(self) -> Dict[str, Dict[str, Any]]:
        await self.connect()
        positions = {}
        for item in self.ib.portfolio():
            positions[item.contract.symbol] = {
                "quantity": item.position,
                "avg_cost": item.averageCost,
                "market_value": item.marketValue,
                "unrealized_pnl": item.unrealizedPNL
            }
        return positions
    
    async def cancel_order(self, order_id: str) -> bool:
        await self.connect()
        for order in self.ib.openOrders():
            if str(order.orderId) == order_id:
                self.ib.cancelOrder(order)
                return True
        return False

class PaperBrokerAdapter(BaseBrokerAdapter):
    def __init__(self, config, notifier, redis):
        self.config = config
        self.notifier = notifier
        self.redis = redis
        self.initial_capital = getattr(config, "paper_trading_capital", 100000)
        self.cash = self.initial_capital
        self.positions = {}
        self.order_id_counter = 0
    
    async def connect(self):
        pass
    
    async def disconnect(self):
        pass
    
    async def get_account_summary(self) -> Dict[str, float]:
        portfolio_value = self.cash + sum(pos["quantity"] * pos["market_price"] for pos in self.positions.values())
        return {"cash": self.cash, "portfolio_value": portfolio_value, "initial_capital": self.initial_capital}
    
    async def place_order(self, order: EnhancedOrder) -> str:
        market_price = await self._get_market_price(order.symbol)
        
        if order.action == "buy":
            cost = order.quantity * (order.limit_price or market_price)
            if cost > self.cash:
                raise ValueError("Insufficient cash")
            self.cash -= cost
            self.positions[order.symbol] = {
                "quantity": self.positions.get(order.symbol, {}).get("quantity", 0) + order.quantity,
                "avg_cost": market_price,
                "market_price": market_price,
                "market_value": self.positions.get(order.symbol, {}).get("quantity", 0) * market_price + cost
            }
        
        elif order.action == "sell":
            if order.symbol not in self.positions or self.positions[order.symbol]["quantity"] < order.quantity:
                raise ValueError("Position not found or insufficient quantity")
            
            self.positions[order.symbol]["quantity"] -= order.quantity
            proceeds = order.quantity * (order.limit_price or market_price)
            self.cash += proceeds
            
            if self.positions[order.symbol]["quantity"] == 0:
                del self.positions[order.symbol]
        
        self.order_id_counter += 1
        order.status = "filled"
        order.executed_price = order.limit_price or market_price
        order.executed_quantity = order.quantity
        
        if self.notifier:
            await self.notifier.send_alert(f"Paper trade executed: {order.symbol} {order.action} {order.quantity}")
        
        return f"paper_{order.symbol}_{self.order_id_counter}"
    
    async def _get_market_price(self, symbol: str) -> float:
        return 150.0
    
    async def get_positions(self) -> Dict[str, Dict[str, Any]]:
        return self.positions
    
    async def cancel_order(self, order_id: str) -> bool:
        return True

def get_broker_adapter(config, notifier=None, redis=None) -> BaseBrokerAdapter:
    if config.execution_mode == "ibkr":
        return IBKRBrokerAdapter(config)
    elif config.execution_mode in ["paper", "alert_only"]:
        return PaperBrokerAdapter(config, notifier, redis)
    else:
        raise ValueError(f"Unsupported execution mode: {config.execution_mode}")
