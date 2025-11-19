import os
from dataclasses import dataclass
from typing import Optional, List
import structlog

logger = structlog.get_logger()

@dataclass
class Config:
    ibkr_host: str = os.getenv("IBKR_HOST", "ibkr_gateway")
    ibkr_port: int = int(os.getenv("IBKR_PORT", "4002"))
    ibkr_client_id: int = int(os.getenv("IBKR_CLIENT_ID", "123"))
    ibkr_readonly: bool = os.getenv("IBKR_READONLY", "true").lower() == "true"
    ibkr_trading_mode: str = os.getenv("IBKR_TRADING_MODE", "paper")
    darwin_account_id: Optional[str] = os.getenv("DARWIN_ACCOUNT_ID")
    alpaca_key: str = os.getenv("ALPACA_API_KEY_ID", "")
    alpaca_secret: str = os.getenv("ALPACA_API_SECRET_KEY", "")
    alpaca_paper: bool = os.getenv("ALPACA_PAPER", "true").lower() == "true"
    paper_trading_capital: float = float(os.getenv("PAPER_TRADING_CAPITAL", "100000"))
    fmp_api_key: Optional[str] = os.getenv("FMP_API_KEY")
    finnhub_api_key: Optional[str] = os.getenv("FINNHUB_API_KEY")
    polygon_key: Optional[str] = os.getenv("POLYGON_API_KEY")
    alpha_key: Optional[str] = os.getenv("ALPHA_VANTAGE_API_KEY")
    tiingo_key: Optional[str] = os.getenv("TIINGO_API_KEY")
    telegram_token: Optional[str] = os.getenv("TELEGRAM_TOKEN")
    telegram_chat_id: Optional[str] = os.getenv("TELEGRAM_CHAT_ID")
    discord_webhook: Optional[str] = os.getenv("DISCORD_WEBHOOK_URL")
    execution_mode: str = os.getenv("FDP_EXECUTION_MODE", "paper")
    max_tickers: int = int(os.getenv("FDP_MAX_TICKERS", "500"))
    min_confidence: float = float(os.getenv("FDP_MIN_CONFIDENCE", "0.75"))
    min_sharpe: float = float(os.getenv("FDP_MIN_SHARPE", "0.5"))
    min_winrate: float = float(os.getenv("FDP_MIN_WINRATE", "0.40"))
    min_trades: int = int(os.getenv("FDP_MIN_TRADES", "20"))
    redis_url: str = os.getenv("FDP_REDIS_URL", "redis://localhost:6379")
    database_url: str = os.getenv("FDP_DATABASE_URL", "")
    daily_api_budget: float = float(os.getenv("FDP_DAILY_API_BUDGET", "5.0"))
    rl_yahoo: int = int(os.getenv("FDP_RL_YAHOO", "2000"))
    rl_alpha: int = int(os.getenv("FDP_RL_ALPHA", "500"))
    rl_tiingo: int = int(os.getenv("FDP_RL_TIINGO", "500"))
    rl_polygon: int = int(os.getenv("FDP_RL_POLYGON", "5"))
    rl_fmp: int = int(os.getenv("FDP_RL_FMP", "300"))
    rl_finnhub: int = int(os.getenv("FDP_RL_FINNHUB", "60"))
    ml_test_size: float = float(os.getenv("FDP_ML_TEST_SIZE", "0.20"))
    ml_optuna_trials: int = int(os.getenv("FDP_ML_OPTUNA_TRIALS", "50"))
    model_retrain_days: int = int(os.getenv("MODEL_RETRAIN_DAYS", "7"))
    ml_drift_threshold: float = float(os.getenv("FDP_ML_DRIFT_THRESHOLD", "0.05"))
    max_position_percent: float = float(os.getenv("FDP_MAX_POSITION_SIZE_PERCENT", "2.0"))
    max_daily_loss_percent: float = float(os.getenv("FDP_MAX_DAILY_LOSS_PERCENT", "2.0"))
    kelly_fraction: float = float(os.getenv("FDP_KELLY_FRACTION", "0.25"))
    kill_switch_enabled: bool = os.getenv("FDP_KILL_SWITCH_ENABLED", "1") == "1"
    kill_switch_file: str = os.getenv("FDP_KILL_SWITCH_FILE", "./data/STOP.txt")
    min_current_ratio: float = float(os.getenv("FDP_MIN_CURRENT_RATIO", "1.0"))
    max_debt_to_equity: float = float(os.getenv("FDP_MAX_DEBT_TO_EQUITY", "2.0"))
    min_gross_margin: float = float(os.getenv("FDP_MIN_GROSS_MARGIN", "0.10"))
    min_operating_margin: float = float(os.getenv("FDP_MIN_OPERATING_MARGIN", "0.05"))
    min_net_margin: float = float(os.getenv("FDP_MIN_NET_MARGIN", "0.05"))
    min_roe: float = float(os.getenv("FDP_MIN_ROE", "0.08"))
    min_roa: float = float(os.getenv("FDP_MIN_ROA", "0.04"))
    min_interest_coverage: float = float(os.getenv("FDP_MIN_INTEREST_COVERAGE", "3.0"))
    prometheus_port: int = int(os.getenv("FDP_PROMETHEUS_PORT", "9090"))
    grafana_port: int = int(os.getenv("FDP_GRAFANA_PORT", "3000"))
    max_concurrent_workers: int = int(os.getenv("FDP_MAX_CONCURRENT_WORKERS", "20"))
    tier_yahoo: str = os.getenv("FDP_TIER_YAHOO", "free")
    tier_alpha: str = os.getenv("FDP_TIER_ALPHA", "free")
    tier_tiingo: str = os.getenv("FDP_TIER_TIINGO", "free")
    tier_finnhub: str = os.getenv("FDP_TIER_FINNHUB", "free")
    tier_polygon: str = os.getenv("FDP_TIER_POLYGON", "premium")
    tier_fmp: str = os.getenv("FDP_TIER_FMP", "free")
    free_limit_yahoo: Optional[int] = int(os.getenv("FDP_FREE_LIMIT_YAHOO", "2000"))
    free_limit_alpha: Optional[int] = int(os.getenv("FDP_FREE_LIMIT_ALPHA", "500"))
    free_limit_tiingo: Optional[int] = int(os.getenv("FDP_FREE_LIMIT_TIINGO", "500"))
    free_limit_polygon: Optional[int] = int(os.getenv("FDP_FREE_LIMIT_POLYGON", "5"))
    free_limit_fmp: Optional[int] = int(os.getenv("FDP_FREE_LIMIT_FMP", "300"))
    free_limit_finnhub: Optional[int] = int(os.getenv("FDP_FREE_LIMIT_FINNHUB", "60"))
    premium_endpoint_polygon: Optional[str] = os.getenv("FDP_PREMIUM_POLYGON_URL")
    premium_endpoint_fmp: Optional[str] = os.getenv("FDP_PREMIUM_FMP_URL")

    def validate(self):
        errors = []
        if self.execution_mode not in ["alert_only", "paper", "ibkr", "alpaca"]:
            errors.append(f"INVALID_EXECUTION_MODE: {self.execution_mode}")
        if not self.database_url:
            errors.append("MISSING_DATABASE_URL")
        if self.execution_mode == "alpaca" and (not self.alpaca_key or not self.alpaca_secret):
            errors.append("MISSING_ALPACA_CREDENTIALS")
        if self.execution_mode == "ibkr" and not self.ibkr_host:
            errors.append("MISSING_IBKR_HOST")
        providers = [self.fmp_api_key, self.finnhub_api_key, self.alpha_key, self.tiingo_key]
        if not any(providers):
            errors.append("NO_DATA_PROVIDER_KEYS")
        if errors:
            raise ConfigValidationError(errors)
        logger.info("config_validated", execution_mode=self.execution_mode, max_tickers=self.max_tickers)
        return True

class ConfigValidationError(Exception):
    def __init__(self, errors: list):
        self.errors = errors
        super().__init__(f"Validation failed: {', '.join(errors)}")

try:
    config = Config()
    config.validate()
except ConfigValidationError as e:
    logger.critical("invalid_configuration", errors=e.errors)
    sys.exit(1)
