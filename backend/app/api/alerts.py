"""
Alerts API - ingestion webhook + query endpoints.
"""
import asyncio
import json
import logging
from typing import Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Request, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.event_bus import event_bus
from app.models.alert_model import Alert
from app.services.ingestion_service import get_ingestion_service

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/alerts/webhook/prometheus", summary="Alertmanager webhook receiver")
async def prometheus_webhook(request: Request):
    """Receive Alertmanager webhook payloads."""
    payload = await request.json()
    svc = get_ingestion_service()
    ids = await svc.ingest_alertmanager_payload(payload)
    return {"ingested": len(ids), "ids": ids}


@router.post("/alerts/webhook/otel", summary="OTEL log webhook receiver")
async def otel_webhook(request: Request):
    """Receive OTLP/HTTP log payloads."""
    payload = await request.json()
    svc = get_ingestion_service()
    ids = await svc.ingest_otel_payload(payload)
    return {"ingested": len(ids), "ids": ids}


@router.get("/alerts", summary="List alerts")
async def list_alerts(
    severity: Optional[str] = Query(None),
    status: Optional[str] = Query(None, enum=["firing", "resolved", "suppressed"]),
    namespace: Optional[str] = Query(None),
    limit: int = Query(100, le=500),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Alert).order_by(desc(Alert.created_at)).limit(limit)
    if severity:
        stmt = stmt.where(Alert.severity == severity)
    if status:
        stmt = stmt.where(Alert.status == status)
    if namespace:
        stmt = stmt.where(Alert.namespace == namespace)

    result = await db.execute(stmt)
    alerts = result.scalars().all()
    return [
        {
            "id": a.id,
            "name": a.name,
            "severity": a.severity,
            "status": a.status,
            "namespace": a.namespace,
            "service": a.service,
            "node": a.node,
            "source_type": a.source_type,
            "labels": a.labels,
            "annotations": a.annotations,
            "starts_at": a.starts_at.isoformat() if a.starts_at else None,
            "correlation_group_id": a.correlation_group_id,
            "created_at": a.created_at.isoformat(),
        }
        for a in alerts
    ]


@router.get("/alerts/stream", summary="SSE stream of real-time alerts")
async def stream_alerts(request: Request):
    """Server-Sent Events stream for real-time alert updates."""
    q = event_bus.subscribe("alerts")

    async def generator():
        try:
            # Replay last 20 events
            for item in event_bus.get_history(20):
                if item.get("channel") == "alerts":
                    yield f"data: {json.dumps(item['data'])}\n\n"
            # Stream new
            while not await request.is_disconnected():
                try:
                    item = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {json.dumps(item['data'])}\n\n"
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        finally:
            event_bus.unsubscribe("alerts", q)

    return StreamingResponse(generator(), media_type="text/event-stream")


@router.get("/alerts/{alert_id}")
async def get_alert(alert_id: str, db: AsyncSession = Depends(get_db)):
    alert = await db.get(Alert, alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    return {
        "id": alert.id, "name": alert.name, "severity": alert.severity,
        "status": alert.status, "namespace": alert.namespace,
        "service": alert.service, "node": alert.node,
        "source_type": alert.source_type, "labels": alert.labels,
        "annotations": alert.annotations, "raw_payload": alert.raw_payload,
        "starts_at": alert.starts_at.isoformat() if alert.starts_at else None,
        "correlation_group_id": alert.correlation_group_id,
        "created_at": alert.created_at.isoformat(),
    }


@router.get("/alerts/stats/summary")
async def alerts_summary(db: AsyncSession = Depends(get_db)):
    """Quick count summary for dashboard."""
    from sqlalchemy import func
    result = await db.execute(
        select(Alert.severity, Alert.status, func.count(Alert.id))
        .group_by(Alert.severity, Alert.status)
    )
    rows = result.all()
    summary = {"total": 0, "by_severity": {}, "by_status": {}, "firing": 0, "critical": 0}
    for severity, status, count in rows:
        summary["total"] += count
        summary["by_severity"][severity] = summary["by_severity"].get(severity, 0) + count
        summary["by_status"][status] = summary["by_status"].get(status, 0) + count
        if status == "firing":
            summary["firing"] += count
        if severity == "critical":
            summary["critical"] += count
    return summary
