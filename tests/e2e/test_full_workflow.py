import pytest
import asyncio
from pathlib import Path
import tempfile
import os
import json

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio, pytest.mark.slow]

@pytest.fixture
def test_env():
    """Create isolated test environment."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Setup test directory
        data_dir = Path(tmpdir) / "data"
        data_dir.mkdir()
        (data_dir / "models").mkdir()
        (data_dir / "logs").mkdir()
        
        # Create test .env
        env_file = Path(tmpdir) / ".env"
        env_content = """
FDP_EXECUTION_MODE=alert_only
FDP_MAX_TICKERS=5
FDP_MIN_CONFIDENCE=0.75
FDP_REDIS_URL=redis://localhost:6379/2
FDP_DATABASE_URL=postgresql://test:test@localhost:5432/findashpro_e2e
FDP_DAILY_API_BUDGET=5.0
ALPHA_VANTAGE_API_KEY=demo
TIINGO_API_KEY=demo
FDP_KILL_SWITCH_ENABLED=0
"""
        env_file.write_text(env_content)
        
        # Set environment
        old_cwd = os.getcwd()
        os.chdir(tmpdir)
        yield tmpdir
        os.chdir(old_cwd)

async def test_e2e_orchestrator_to_dashboard(test_env, redis_client):
    """End-to-end test from orchestrator to dashboard."""
    # Load environment
    from dotenv import load_dotenv
    load_dotenv()
    
    # Initialize orchestrator
    from fdp.core.orchestrator import FinDashProOrchestrator
    from fdp.core.config import config
    
    orchestrator = FinDashProOrchestrator()
    orchestrator.running = True
    
    # Mock ticker universe
    test_universe = [
        {"symbol": "TEST", "region": "usa", "type": "stock"},
    ]
    
    # Run producer
    await orchestrator.init_db_pool()
    await orchestrator.init_redis_streams()
    
    asyncio.create_task(orchestrator.producer(pd.DataFrame(test_universe)))
    
    # Wait for processing
    await asyncio.sleep(3)
    
    # Check signal was generated
    signals = await redis_client.llen("signals:queue")
    assert signals > 0
    
    # Check dashboard can read
    signal_data = await redis_client.lindex("signals:queue", 0)
    signal = json.loads(signal_data)
    
    assert "ticker" in signal
    assert "action" in signal
    assert "confidence" in signal
    
    await orchestrator.close_db_pool()

async def test_e2e_drift_detection(test_env, redis_client):
    """Test drift detection end-to-end."""
    from fdp.ml.ops.drift_monitor import DriftMonitor
    
    monitor = DriftMonitor(redis_client)
    
    # Create test data
    ref_data = pd.DataFrame({
        'feature1': np.random.randn(100),
        'feature2': np.random.randn(100)
    })
    
    # Set reference
    await redis_client.set("drift:reference:TEST", ref_data.to_json())
    
    # Test non-drift data
    new_data = pd.DataFrame({
        'feature1': np.random.randn(50) * 1.1,  # Small variance
        'feature2': np.random.randn(50) * 1.1
    })
    
    result = await monitor.check("TEST", new_data)
    assert result["drift_detected"] is False
    
    # Test drift data
    drift_data = pd.DataFrame({
        'feature1': np.random.randn(50) + 5,  # Large shift
        'feature2': np.random.randn(50) * 0.5
    })
    
    result = await monitor.check("TEST", drift_data)
    assert result["drift_detected"] is True
    assert len(result["warnings"]) > 0
