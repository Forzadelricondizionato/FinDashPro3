import pandas as pd
import numpy as np
from typing import Dict, Any, List
from datetime import datetime, timedelta
import asyncio
import structlog
from fdp.core.config import config

logger = structlog.get_logger()

class BacktestEngine:
    """Simple backtesting engine for strategy validation."""
    
    def __init__(self, redis: Any):
        self.redis = redis
        self.results: Dict[str, Any] = {}
    
    async def run_backtest(self, ticker: str, signals: List[Dict], initial_capital: float = 10000) -> Dict:
        """Run backtest on historical signals."""
        try:
            # Load historical data
            ohlcv = await self._load_historical_data(ticker)
            if ohlcv.empty:
                return {"error": "No historical data"}
            
            # Simulate trading
            portfolio = initial_capital
            position = 0
            trades = []
            
            for signal in sorted(signals, key=lambda x: x['timestamp']):
                signal_time = datetime.fromisoformat(signal['timestamp'])
                
                # Find closest price
                price_data = ohlcv[ohlcv['date'] <= signal_time].tail(1)
                if price_data.empty:
                    continue
                
                price = price_data.iloc[0]['close']
                
                if signal['action'] == 'buy' and position == 0:
                    # Buy
                    position_size = (portfolio * 0.95) / price  # Use 95% of portfolio
                    cost = position_size * price
                    portfolio -= cost
                    position = position_size
                    
                    trades.append({
                        'type': 'buy',
                        'price': price,
                        'quantity': position_size,
                        'timestamp': signal_time
                    })
                
                elif signal['action'] == 'sell' and position > 0:
                    # Sell
                    revenue = position * price
                    portfolio += revenue
                    
                    # Calculate P&L
                    buy_trade = next((t for t in trades if t['type'] == 'buy'), None)
                    if buy_trade:
                        pnl = revenue - (buy_trade['quantity'] * buy_trade['price'])
                        trades.append({
                            'type': 'sell',
                            'price': price,
                            'quantity': position,
                            'pnl': pnl,
                            'timestamp': signal_time
                        })
                    
                    position = 0
            
            # Final portfolio value
            final_value = portfolio
            if position > 0:
                final_price = ohlcv.iloc[-1]['close']
                final_value += position * final_price
            
            # Calculate metrics
            total_pnl = final_value - initial_capital
            returns = (total_pnl / initial_capital) * 100
            
            winning_trades = len([t for t in trades if t.get('pnl', 0) > 0])
            total_trades = len([t for t in trades if t['type'] == 'sell'])
            win_rate = winning_trades / total_trades if total_trades > 0 else 0
            
            result = {
                'ticker': ticker,
                'initial_capital': initial_capital,
                'final_value': final_value,
                'total_pnl': total_pnl,
                'returns_percent': returns,
                'win_rate': win_rate,
                'total_trades': total_trades,
                'trades': trades
            }
            
            # Store result
            key = f"backtest:{ticker}:{datetime.now().isoformat()}"
            await self.redis.setex(key, 86400 * 7, str(result))
            
            self.results[ticker] = result
            logger.info("backtest_completed", ticker=ticker, returns=returns)
            
            return result
            
        except Exception as e:
            logger.error("backtest_failed", ticker=ticker, error=str(e))
            return {"error": str(e)}
    
    async def _load_historical_data(self, ticker: str) -> pd.DataFrame:
        """Load historical data for backtesting."""
        # For now, use yfinance as source
        import yfinance as yf
        
        try:
            ticker_obj = yf.Ticker(ticker)
            hist = ticker_obj.history(period="1y")
            hist = hist.reset_index()
            hist.columns = [col.lower() for col in hist.columns]
            return hist
        except Exception as e:
            logger.error("backtest_data_load_failed", ticker=ticker, error=str(e))
            return pd.DataFrame()
    
    async def get_backtest_results(self, ticker: str = None) -> Dict:
        """Get backtest results from Redis."""
        if ticker:
            pattern = f"backtest:{ticker}:*"
        else:
            pattern = "backtest:*"
        
        keys = await self.redis.keys(pattern)
        results = []
        
        for key in keys:
            data = await self.redis.get(key)
            if data:
                try:
                    results.append(eval(data))  # Simple eval for dict
                except:
                    pass
        
        return {
            "total_backtests": len(results),
            "results": sorted(results, key=lambda x: x.get('returns_percent', 0), reverse=True)
        }
