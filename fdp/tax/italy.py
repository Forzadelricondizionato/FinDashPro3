# fdp/tax/italy.py
import pandas as pd
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List

class ItalianTaxReporter:
    def __init__(self, db_path: str, output_dir: Path):
        self.db_path = db_path
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_730_report(self, year: int, method: str = "LIFO") -> Path:
        con = sqlite3.connect(self.db_path)
        
        trades = pd.read_sql("""
            SELECT symbol, action, quantity, price, executed_at, fees
            FROM trades
            WHERE strftime('%Y', executed_at) = ?
        """, con, params=(str(year),))
        
        if trades.empty:
            con.close()
            return Path()
        
        pf = pd.read_sql("""
            SELECT symbol, market_value, quantity
            FROM positions
            WHERE date = (SELECT MAX(date) FROM positions)
        """, con)
        
        ivafe = pf["market_value"].sum() * 0.002
        
        report = []
        for symbol in trades["symbol"].unique():
            sym_trades = trades[trades["symbol"] == symbol].sort_values("executed_at")
            plusvalenze = self._calculate_plusvalenze(sym_trades, method)
            
            pf_value = pf[pf["symbol"] == symbol]["market_value"].iloc[0] if symbol in pf["symbol"].values else 0
            report.append({
                "codice": symbol,
                "plusvalenze": plusvalenze,
                "ivafe": pf_value * 0.002
            })
        
        df = pd.DataFrame(report)
        output_path = self.output_dir / f"730_{year}.csv"
        df.to_csv(output_path, index=False, columns=["codice", "plusvalenze", "ivafe"])
        
        con.close()
        return output_path
    
    def _calculate_plusvalenze(self, trades: pd.DataFrame, method: str) -> float:
        if method == "LIFO":
            buys = trades[trades["action"] == "buy"]["price"].tolist()
            sells = trades[trades["action"] == "sell"]["price"].tolist()
            plusvalenze = 0
            for sell_price in sells:
                if buys:
                    buy_price = buys.pop()
                    plusvalenze += (sell_price - buy_price)
            return plusvalenze
        return 0
