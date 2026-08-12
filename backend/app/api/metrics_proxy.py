"""metrics_proxy.py - Proxy PromQL queries from the UI to Prometheus.

Queries are routed to the first configured Prometheus DataSource (matching the
Sources UI). If none is configured, falls back to the PROMETHEUS_URL env var.
"""
from fastapi import APIRouter, Query
from typing import Dict, Optional
from sqlalchemy import select

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.models.alert_model import DataSource
from app.services.prometheus_client import PrometheusClient

router = APIRouter()

# Cache one client per endpoint so we don't rebuild connections per request.
_clients: Dict[str, PrometheusClient] = {}


async def _client() -> PrometheusClient:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(DataSource)
            .where(DataSource.source_type == "prometheus")
            .order_by(DataSource.created_at)
        )
        src = result.scalars().first()
    endpoint = src.endpoint if src and src.endpoint else settings.PROMETHEUS_URL
    client = _clients.get(endpoint)
    if client is None:
        client = PrometheusClient(base_url=endpoint)
        _clients[endpoint] = client
    return client


@router.get("/metrics/query")
async def proxy_query(q: str = Query(..., alias="query"), t: Optional[str] = Query(None, alias="time")):
    result = await (await _client()).instant_query(q, t)
    return {"result": result}


@router.get("/metrics/query_range")
async def proxy_range(
    q: str = Query(..., alias="query"),
    start: str = Query(...),
    end: str = Query(...),
    step: str = Query("60s"),
):
    result = await (await _client()).range_query(q, start, end, step)
    return {"result": result}


@router.get("/metrics/list")
async def list_metrics():
    metrics = await (await _client()).list_metrics()
    return {"metrics": metrics}
