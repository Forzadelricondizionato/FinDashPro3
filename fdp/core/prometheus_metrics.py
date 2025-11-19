from prometheus_client import Counter, Histogram, Gauge, Info
import psutil
import time

METRICS = {
    "processed_tickers": Counter("fdp_processed_tickers_total", "Total tickers processed"),
    "failed_tickers": Counter("fdp_failed_tickers_total", "Total tickers failed"),
    "signals_total": Counter("fdp_signals_total", "Total signals generated"),
    "api_costs": Gauge("fdp_api_costs_daily", "Daily API costs"),
    "circuit_breaker_state": Gauge("fdp_circuit_breaker_state", "Circuit breaker state", ["provider"]),
    "drift_detected": Gauge("fdp_drift_detected", "Drift detection flag", ["ticker"]),
    "portfolio_value": Gauge("fdp_portfolio_value", "Current portfolio value"),
    "position_size_bytes": Histogram("fdp_position_size_bytes", "Position size in bytes"),
    "inference_duration": Histogram("fdp_inference_duration_seconds", "ML inference duration"),
    "order_execution_duration": Histogram("fdp_order_execution_duration_seconds", "Order execution duration")
}

def update_portfolio_value(value: float):
    METRICS["portfolio_value"].set(value)

def increment_api_costs(amount: float):
    METRICS["api_costs"].inc(amount)

