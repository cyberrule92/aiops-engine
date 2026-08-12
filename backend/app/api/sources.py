"""sources.py - Data source connection management"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import httpx
from app.core.config import settings
from app.core.database import get_db
from app.models.alert_model import DataSource

router = APIRouter()

# Prometheus web-UI routes that users often paste by mistake. These are SPA
# pages, not API/base paths, so strip them to recover the true base URL.
_PROM_UI_SUFFIXES = ("/query", "/graph", "/targets", "/alerts", "/rules", "/flags")


def _normalize_prometheus_base(endpoint: str) -> str:
    """Return the Prometheus base URL, stripping trailing slashes and UI routes."""
    base = endpoint.strip().rstrip("/")
    for suffix in _PROM_UI_SUFFIXES:
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    return base.rstrip("/")


class SourceCreate(BaseModel):
    name: str
    source_type: str  # prometheus | otel
    endpoint: str
    config: Optional[dict] = None


@router.post("/sources")
async def create_source(req: SourceCreate, db: AsyncSession = Depends(get_db)):
    # Normalize the endpoint so we persist the true base URL, not a UI route.
    if req.source_type == "prometheus":
        endpoint = _normalize_prometheus_base(req.endpoint)
    else:
        endpoint = req.endpoint.strip().rstrip("/")

    # Test connectivity
    status = "pending"
    error = None
    try:
        async with httpx.AsyncClient(timeout=5, verify=settings.VERIFY_TLS) as client:
            if req.source_type == "prometheus":
                resp = await client.get(f"{endpoint}/-/healthy")
                status = "ok" if resp.status_code == 200 else "error"
                if status == "error":
                    error = f"GET {endpoint}/-/healthy returned HTTP {resp.status_code}"
            elif req.source_type == "otel":
                resp = await client.get(f"{endpoint}/")
                status = "ok" if resp.status_code < 500 else "error"
                if status == "error":
                    error = f"GET {endpoint}/ returned HTTP {resp.status_code}"
    except Exception as e:
        status = "error"
        error = str(e)[:256]

    src = DataSource(
        name=req.name,
        source_type=req.source_type,
        endpoint=endpoint,
        status=status,
        error_message=error,
        config=req.config,
    )
    db.add(src)
    await db.commit()
    await db.refresh(src)
    return {
        "id": src.id, "name": src.name, "source_type": src.source_type,
        "endpoint": src.endpoint, "status": src.status,
        "error_message": src.error_message,
        "created_at": src.created_at.isoformat(),
    }


@router.get("/sources")
async def list_sources(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DataSource))
    sources = result.scalars().all()
    return [
        {
            "id": s.id, "name": s.name, "source_type": s.source_type,
            "endpoint": s.endpoint, "status": s.status,
            "error_message": s.error_message,
            "last_scrape": s.last_scrape.isoformat() if s.last_scrape else None,
            "created_at": s.created_at.isoformat(),
        }
        for s in sources
    ]


@router.post("/sources/{source_id}/test")
async def test_source(source_id: str, db: AsyncSession = Depends(get_db)):
    src = await db.get(DataSource, source_id)
    if not src:
        raise HTTPException(404, "Source not found")
    try:
        async with httpx.AsyncClient(timeout=5, verify=settings.VERIFY_TLS) as client:
            if src.source_type == "prometheus":
                base = _normalize_prometheus_base(src.endpoint)
                resp = await client.get(f"{base}/-/healthy")
                ok = resp.status_code == 200
            else:
                resp = await client.get(src.endpoint)
                ok = resp.status_code < 500
        src.status = "ok" if ok else "error"
        src.error_message = None if ok else f"HTTP {resp.status_code}"
    except Exception as e:
        src.status = "error"
        src.error_message = str(e)[:256]
        ok = False
    await db.commit()
    return {"status": src.status, "reachable": ok, "error_message": src.error_message}


@router.delete("/sources/{source_id}")
async def delete_source(source_id: str, db: AsyncSession = Depends(get_db)):
    src = await db.get(DataSource, source_id)
    if not src:
        raise HTTPException(404, "Not found")
    await db.delete(src)
    await db.commit()
    return {"deleted": True}
