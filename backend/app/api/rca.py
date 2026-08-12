"""rca.py"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.models.alert_model import RCAReport
from app.services.rca_service import get_rca_service

router = APIRouter()


class ManualRCARequest(BaseModel):
    alert_ids: Optional[List[str]] = []
    anomaly_ids: Optional[List[str]] = []
    correlation_group_id: Optional[str] = None
    extra_context: Optional[str] = None


@router.post("/rca/generate")
async def generate_rca(req: ManualRCARequest, db: AsyncSession = Depends(get_db)):
    """Trigger RCA for a correlation group or ad-hoc alert set."""
    svc = get_rca_service()

    if req.correlation_group_id:
        try:
            result = await svc.generate_rca_for_group(req.correlation_group_id)
        except ValueError as e:
            raise HTTPException(404, str(e))
    else:
        from app.models.alert_model import Alert, Anomaly
        alerts, anomalies = [], []
        if req.alert_ids:
            res = await db.execute(select(Alert).where(Alert.id.in_(req.alert_ids)))
            for a in res.scalars().all():
                alerts.append({"id": a.id, "name": a.name, "severity": a.severity,
                    "namespace": a.namespace, "service": a.service, "node": a.node,
                    "labels": a.labels, "annotations": a.annotations,
                    "starts_at": a.starts_at.isoformat() if a.starts_at else None})
        if req.anomaly_ids:
            res = await db.execute(select(Anomaly).where(Anomaly.id.in_(req.anomaly_ids)))
            for a in res.scalars().all():
                anomalies.append({"id": a.id, "metric_name": a.metric_name,
                    "score": a.score, "metric_value": a.metric_value,
                    "baseline_value": a.baseline_value, "deviation_pct": a.deviation_pct,
                    "service": a.service, "namespace": a.namespace,
                    "detected_at": a.detected_at.isoformat() if a.detected_at else None})
        result = await svc.generate_rca(alerts=alerts, anomalies=anomalies)

    return result


@router.get("/rca")
async def list_rca_reports(limit: int = 50, db: AsyncSession = Depends(get_db)):
    # Hide reaped/failed placeholders; show completed + genuinely in-progress.
    stmt = (
        select(RCAReport)
        .where(RCAReport.status != "failed")
        .order_by(desc(RCAReport.created_at))
        .limit(limit)
    )
    result = await db.execute(stmt)
    reports = result.scalars().all()
    return [_serialize(r) for r in reports]


def _serialize(r: RCAReport, include_raw: bool = False) -> dict:
    out = {
        "id": r.id,
        "probable_root_cause": r.root_cause,
        "root_cause": r.root_cause,
        "confidence": r.confidence,
        "llm_model": r.llm_model,
        "problem_overview": r.problem_overview or "",
        "impact": r.impact or "",
        "timeline": r.timeline or [],
        "supporting_evidence": r.supporting_evidence or [],
        "hypotheses": r.hypotheses or [],
        "reasoning": r.reasoning or "",
        "short_term_actions": r.short_term_actions or [],
        "long_term_actions": r.long_term_actions or [],
        "contributing_factors": r.contributing_factors or [],
        "remediation_steps": r.remediation_steps or [],
        "related_components": r.related_components or [],
        "correlation_group_id": r.correlation_group_id,
        "alert_id": r.alert_id,
        "anomaly_id": r.anomaly_id,
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "status": r.status,
    }
    if include_raw:
        out["raw_llm_response"] = r.raw_llm_response
    return out


@router.get("/rca/{report_id}")
async def get_rca(report_id: str, db: AsyncSession = Depends(get_db)):
    r = await db.get(RCAReport, report_id)
    if not r:
        raise HTTPException(404, "Not found")
    return _serialize(r, include_raw=True)
