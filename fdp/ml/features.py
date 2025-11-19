import pandas as pd
import numpy as np
from typing import Tuple, Dict, Any
import structlog

logger = structlog.get_logger()

class FeatureEngineering:
    def __init__(self):
        self.lookback_periods = [5, 10, 20, 50, 200]

    def engineer_features(self, ohlcv: pd.DataFrame, fundamentals: Dict[str, Any], sentiment: float) -> Tuple[pd.DataFrame, pd.Series]:
        if ohlcv.empty:
            return pd.DataFrame(), pd.Series()
        df = ohlcv.copy()
        df = self._add_technical_indicators(df)
        df = self._add_fundamental_features(df, fundamentals)
        df['sentiment'] = sentiment
        df = self._add_lag_features(df)
        df = self._add_volatility_features(df)
        target = self._create_target(df)
        feature_cols = [col for col in df.columns if col not in ['target', 'date', 'symbol']]
        return df[feature_cols], target

    def _add_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        for period in self.lookback_periods:
            df[f'sma_{period}'] = df['close'].rolling(window=period).mean()
            df[f'ema_{period}'] = df['close'].ewm(span=period).mean()
            df[f'rsi_{period}'] = self._calculate_rsi(df['close'], period)
            df[f'momentum_{period}'] = df['close'].pct_change(period)
        df['macd'], df['macd_signal'] = self._calculate_macd(df['close'])
        df['bollinger_upper'], df['bollinger_lower'] = self._calculate_bollinger(df['close'])
        return df

    def _calculate_rsi(self, prices: pd.Series, period: int) -> pd.Series:
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    def _calculate_macd(self, prices: pd.Series) -> Tuple[pd.Series, pd.Series]:
        ema12 = prices.ewm(span=12).mean()
        ema26 = prices.ewm(span=26).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9).mean()
        return macd, signal

    def _calculate_bollinger(self, prices: pd.Series, period: int = 20) -> Tuple[pd.Series, pd.Series]:
        sma = prices.rolling(window=period).mean()
        std = prices.rolling(window=period).std()
        return sma + (std * 2), sma - (std * 2)

    def _add_fundamental_features(self, df: pd.DataFrame, fundamentals: Dict[str, Any]) -> pd.DataFrame:
        df['pe_ratio'] = fundamentals.get('priceEarningsRatio', np.nan)
        df['pb_ratio'] = fundamentals.get('priceToBookRatio', np.nan)
        df['debt_equity'] = fundamentals.get('debtEquityRatio', np.nan)
        df['roe'] = fundamentals.get('returnOnEquity', np.nan)
        df['current_ratio'] = fundamentals.get('currentRatio', np.nan)
        return df.ffill().bfill()

    def _add_lag_features(self, df: pd.DataFrame) -> pd.DataFrame:
        for lag in [1, 2, 5, 10]:
            df[f'close_lag_{lag}'] = df['close'].shift(lag)
            df[f'volume_lag_{lag}'] = df['volume'].shift(lag)
        return df

    def _add_volatility_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df['volatility_5d'] = df['close'].pct_change().rolling(5).std()
        df['volatility_30d'] = df['close'].pct_change().rolling(30).std()
        df['volatility_200d'] = df['close'].pct_change().rolling(200).std()
        return df

    def _create_target(self, df: pd.DataFrame) -> pd.Series:
        df['target'] = (df['close'].shift(-5) > df['close']).astype(int)
        return df['target']
