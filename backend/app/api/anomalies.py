"""anomalies.py - Anomaly query endpoints"""
import asyncio, json, logging
from typing import Optional
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.core.event_bus import event_bus
from app.models.alert_model import Anomaly

router = APIRouter()
logger = logging.getLogger(__name__)


def _anomaly_dict(a: Anomaly) -> dict:
    return {
        "id": a.id, "metric_name": a.metric_name, "score": a.score,
        "zscore": a.zscore, "method": a.method,
        "metric_value": a.metric_value, "baseline_value": a.baseline_value,
        "deviation_pct": a.deviation_pct, "namespace": a.namespace,
        "service": a.service, "node": a.node, "labels": a.labels,
        "context": a.context, "acknowledged": a.acknowledged,
        "kind": a.kind or "observed",
        "prediction_status": a.prediction_status,
        "predicted_breach_at": a.predicted_breach_at.isoformat() if a.predicted_breach_at else None,
        "detected_at": a.detected_at.isoformat() if a.detected_at else None,
    }


@router.get("/anomalies")
async def list_anomalies(
    min_score: float = Query(0.0),
    namespace: Optional[str] = Query(None),
    kind: Optional[str] = Query(None, description="observed | predicted"),
    limit: int = Query(100, le=500),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Anomaly).order_by(desc(Anomaly.detected_at)).limit(limit)
    if min_score > 0:
        stmt = stmt.where(Anomaly.score >= min_score)
    if namespace:
        stmt = stmt.where(Anomaly.namespace == namespace)
    if kind in ("observed", "predicted"):
        stmt = stmt.where(Anomaly.kind == kind)
    result = await db.execute(stmt)
    return [_anomaly_dict(a) for a in result.scalars().all()]


@router.get("/anomalies/predicted")
async def list_predicted(
    pending_only: bool = Query(True),
    limit: int = Query(100, le=500),
    db: AsyncSession = Depends(get_db),
):
    """Forecasted (not-yet-occurred) anomalies, soonest breach first."""
    stmt = select(Anomaly).where(Anomaly.kind == "predicted")
    if pending_only:
        stmt = stmt.where(Anomaly.prediction_status == "pending")
    stmt = stmt.order_by(Anomaly.predicted_breach_at.asc().nulls_last()).limit(limit)
    result = await db.execute(stmt)
    return [_anomaly_dict(a) for a in result.scalars().all()]


@router.get("/anomalies/stream")
async def stream_anomalies(request: Request):
    q = event_bus.subscribe("anomalies")
    async def generator():
        try:
            while not await request.is_disconnected():
                try:
                    item = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {json.dumps(item['data'])}\n\n"
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        finally:
            event_bus.unsubscribe("anomalies", q)
    return StreamingResponse(generator(), media_type="text/event-stream")


@router.post("/anomalies/{anomaly_id}/acknowledge")
async def ack_anomaly(anomaly_id: str, db: AsyncSession = Depends(get_db)):
    anomaly = await db.get(Anomaly, anomaly_id)
    if not anomaly:
        from fastapi import HTTPException
        raise HTTPException(404, "Not found")
    anomaly.acknowledged = True
    await db.commit()
    return {"acknowledged": True}
