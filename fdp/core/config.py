import os
import hvac
from pydantic import BaseModel, Field, validator
from typing import Optional, List
from pathlib import Path
import structlog

logger = structlog.get_logger()

class ConfigValidationError(Exception):
    pass

class Config(BaseModel):
    execution_mode: str = Field(..., regex="^(paper|alert_only|ibkr|alpaca)$")
    max_tickers: int = Field(default=50, ge=1, le=1000)
    min_confidence: float = Field(default=0.75, ge=0.0, le=1.0)
    redis_url: str
    database_url: str
    daily_api_budget: float = Field(default=5.0, ge=0.0)
    vault_addr: str = "http://localhost:8200"
    vault_token: Optional[str] = None
    kill_switch_enabled: bool = False
    kill_switch_file: Optional[str] = None
    kill_switch_token: Optional[str] = None
    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 4001
    ibkr_client_id: int = 1
    telegram_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    discord_webhook: Optional[str] = None
    newsapi_key: Optional[str] = None
    min_current_ratio: float = 1.0
    max_debt_to_equity: float = 2.0
    min_roe: float = 0.08
    
    @validator("execution_mode")
    def validate_execution_mode(cls, v):
        if v not in ["paper", "alert_only", "ibkr", "alpaca"]:
            raise ConfigValidationError("INVALID_EXECUTION_MODE")
        return v
    
    @validator("database_url")
    def validate_database_url(cls, v):
        if not v or v == "":
            raise ConfigValidationError("MISSING_DATABASE_URL")
        return v
    
    def load_secrets_from_vault(self):
        if not self.vault_token:
            return
        client = hvac.Client(url=self.vault_addr, token=self.vault_token)
        if not client.is_authenticated():
            logger.error("Vault authentication failed")
            return
        secret_path = "fdp/secrets"
        try:
            secret = client.secrets.kv.v2.read_secret_version(path=secret_path)
            data = secret["data"]["data"]
            for key, value in data.items():
                if hasattr(self, key) and value:
                    setattr(self, key, value)
        except Exception as e:
            logger.error("Vault read failed", error=str(e))
    
    @classmethod
    def from_env(cls):
        config = cls(
            execution_mode=os.getenv("FDP_EXECUTION_MODE", "paper"),
            max_tickers=int(os.getenv("FDP_MAX_TICKERS", "50")),
            min_confidence=float(os.getenv("FDP_MIN_CONFIDENCE", "0.75")),
            redis_url=os.getenv("FDP_REDIS_URL", "redis://localhost:6379/0"),
            database_url=os.getenv("FDP_DATABASE_URL", ""),
            daily_api_budget=float(os.getenv("FDP_DAILY_API_BUDGET", "5.0")),
            vault_addr=os.getenv("VAULT_ADDR", "http://localhost:8200"),
            vault_token=os.getenv("VAULT_TOKEN"),
            kill_switch_enabled=os.getenv("FDP_KILL_SWITCH_ENABLED", "0") == "1",
            kill_switch_file=os.getenv("FDP_KILL_SWITCH_FILE"),
            kill_switch_token=os.getenv("FDP_KILL_SWITCH_TOKEN"),
            ibkr_host=os.getenv("IBKR_HOST", "127.0.0.1"),
            ibkr_port=int(os.getenv("IBKR_PORT", "4001")),
            ibkr_client_id=int(os.getenv("IBKR_CLIENT_ID", "1")),
            telegram_token=os.getenv("TELEGRAM_TOKEN"),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID"),
            discord_webhook=os.getenv("DISCORD_WEBHOOK"),
            newsapi_key=os.getenv("NEWSAPI_KEY"),
            min_current_ratio=float(os.getenv("FDP_MIN_CURRENT_RATIO", "1.0")),
            max_debt_to_equity=float(os.getenv("FDP_MAX_DEBT_TO_EQUITY", "2.0")),
            min_roe=float(os.getenv("FDP_MIN_ROE", "0.08"))
        )
        config.load_secrets_from_vault()
        return config

config = Config.from_env()
