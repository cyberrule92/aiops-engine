"""correlation.py"""
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.models.alert_model import CorrelationGroup, Alert
from app.services.correlation_service import get_correlation_service

router = APIRouter()
logger = logging.getLogger(__name__)


def _alert_to_dict(a: Alert) -> dict:
    return {
        "id": a.id,
        "name": a.name,
        "severity": a.severity,
        "status": a.status,
        "namespace": a.namespace,
        "service": a.service,
        "node": a.node,
        "labels": a.labels,
        "annotations": a.annotations,
        "starts_at": a.starts_at.isoformat() if a.starts_at else None,
    }


async def _embed_alerts(db: AsyncSession, group: CorrelationGroup) -> list:
    """Load full member alert details for a group, ordered by start time."""
    ids = group.alert_ids or []
    if not ids:
        return []
    res = await db.execute(select(Alert).where(Alert.id.in_(ids)))
    alerts = res.scalars().all()
    alerts.sort(key=lambda a: a.starts_at or a.id)
    return [_alert_to_dict(a) for a in alerts]


def _group_to_dict(g: CorrelationGroup, alerts: list) -> dict:
    return {
        "id": g.id,
        "root_alert_id": g.root_alert_id,
        "alert_ids": g.alert_ids,
        "anomaly_ids": g.anomaly_ids,
        "similarity_score": g.similarity_score,
        "pattern": g.pattern,
        "labels": g.labels,
        "inference": g.inference,
        "inference_status": g.inference_status,
        "root_entity": g.root_entity,
        "topology": g.topology,
        "window_start": g.window_start.isoformat() if g.window_start else None,
        "window_end": g.window_end.isoformat() if g.window_end else None,
        "created_at": g.created_at.isoformat(),
        "alerts": alerts,
    }


@router.get("/correlations")
async def list_correlations(
    limit: int = Query(50, le=200),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(CorrelationGroup).order_by(desc(CorrelationGroup.created_at)).limit(limit)
    result = await db.execute(stmt)
    groups = result.scalars().all()
    out = []
    for g in groups:
        out.append(_group_to_dict(g, await _embed_alerts(db, g)))
    return out


@router.get("/correlations/{group_id}")
async def get_correlation(group_id: str, db: AsyncSession = Depends(get_db)):
    g = await db.get(CorrelationGroup, group_id)
    if not g:
        raise HTTPException(404, "Not found")
    return _group_to_dict(g, await _embed_alerts(db, g))


@router.post("/correlations/{group_id}/infer")
async def reinfer_correlation(group_id: str):
    """Manually (re)generate the llama inference alert for a correlation group."""
    try:
        return await get_correlation_service().regenerate_inference(group_id)
    except ValueError as e:
        raise HTTPException(404, str(e))
