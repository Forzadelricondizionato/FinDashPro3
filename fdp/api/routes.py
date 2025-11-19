from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Dict, Any
import asyncio

router = APIRouter()

class SignalRequest(BaseModel):
    tickers: List[str]

class SignalResponse(BaseModel):
    signals: List[Dict[str, Any]]

@router.post("/signals/generate", response_model=SignalResponse)
async def generate_signals(request: SignalRequest):
    orchestrator = get_orchestrator()
    tasks = [orchestrator.process_ticker_safe(ticker, "usa", "stock") for ticker in request.tickers[:10]]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    signals = [r for r in results if isinstance(r, dict) and r.get("status") == "success"]
    return SignalResponse(signals=signals)

@router.get("/positions")
async def get_positions():
    orchestrator = get_orchestrator()
    positions = await orchestrator.broker.get_positions()
    return positions

@router.get("/account")
async def get_account():
    orchestrator = get_orchestrator()
    summary = await orchestrator.broker.get_account_summary()
    return summary

@router.post("/order")
async def place_order(order: Dict[str, Any]):
    orchestrator = get_orchestrator()
    enhanced_order = EnhancedOrder(**order)
    validation = orchestrator.risk.validate_order(enhanced_order)
    if not validation["allowed"]:
        raise HTTPException(status_code=400, detail=validation["reason"])
    order_id = await orchestrator.broker.place_order(enhanced_order)
    return {"order_id": order_id}

def get_orchestrator():
    from main import orchestrator
    return orchestrator

