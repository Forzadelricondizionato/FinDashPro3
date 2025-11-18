"""Test configuration validation."""
import pytest
from fdp.core.config import Config, ConfigValidationError

def test_config_validation_pass(test_config):
    """Test valid configuration."""
    assert test_config.validate() is True

def test_invalid_execution_mode():
    """Test invalid execution mode."""
    config = Config(execution_mode="invalid")
    with pytest.raises(ConfigValidationError) as exc:
        config.validate()
    assert "INVALID_EXECUTION_MODE" in str(exc.value)

def test_missing_database_url():
    """Test missing database URL."""
    config = Config(execution_mode="paper", database_url="")
    with pytest.raises(ConfigValidationError) as exc:
        config.validate()
    assert "MISSING_DATABASE_URL" in str(exc.value)

def test_missing_alpaca_credentials():
    """Test missing Alpaca credentials."""
    config = Config(
        execution_mode="alpaca",
        database_url="test",
        alpaca_key="",
        alpaca_secret=""
    )
    with pytest.raises(ConfigValidationError) as exc:
        config.validate()
    assert "MISSING_ALPACA_CREDENTIALS" in str(exc.value)

def test_no_data_providers():
    """Test no data providers."""
    config = Config(
        execution_mode="paper",
        database_url="test",
        fmp_api_key=None,
        finnhub_api_key=None,
        alpha_key=None,
        tiingo_key=None
    )
    with pytest.raises(ConfigValidationError) as exc:
        config.validate()
    assert "NO_DATA_PROVIDER_KEYS" in str(exc.value)

def test_fundamentals_validation_defaults(test_config):
    """Test fundamentals validation defaults."""
    assert test_config.min_current_ratio == 1.0
    assert test_config.max_debt_to_equity == 2.0
    assert test_config.min_roe == 0.08
