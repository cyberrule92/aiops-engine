"""health.py"""
import httpx
from fastapi import APIRouter
from sqlalchemy import select

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.models.alert_model import DataSource

router = APIRouter()


async def _prometheus_base() -> str:
    """First configured Prometheus source, or the PROMETHEUS_URL env fallback."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(DataSource)
            .where(DataSource.source_type == "prometheus")
            .order_by(DataSource.created_at)
        )
        src = result.scalars().first()
    return src.endpoint if src and src.endpoint else settings.PROMETHEUS_URL


@router.get("/health")
async def health():
    return {"status": "ok", "service": "aiops-engine"}


@router.get("/health/full")
async def full_health():
    checks = {}

    # Prometheus
    try:
        base = (await _prometheus_base()).rstrip("/")
        async with httpx.AsyncClient(timeout=3, verify=settings.VERIFY_TLS) as c:
            r = await c.get(f"{base}/-/healthy")
            checks["prometheus"] = "ok" if r.status_code == 200 else "degraded"
    except Exception:
        checks["prometheus"] = "unreachable"

    # Ollama
    try:
        async with httpx.AsyncClient(timeout=3, verify=settings.VERIFY_TLS) as c:
            r = await c.get(f"{settings.OLLAMA_BASE_URL}/api/tags")
            checks["ollama"] = "ok" if r.status_code == 200 else "degraded"
    except Exception:
        checks["ollama"] = "unreachable"

    # Alertmanager
    try:
        async with httpx.AsyncClient(timeout=3, verify=settings.VERIFY_TLS) as c:
            r = await c.get(f"{settings.ALERTMANAGER_URL}/-/healthy")
            checks["alertmanager"] = "ok" if r.status_code == 200 else "degraded"
    except Exception:
        checks["alertmanager"] = "unreachable"

    overall = "ok" if all(v == "ok" for v in checks.values()) else "degraded"
    return {"status": overall, "checks": checks}
