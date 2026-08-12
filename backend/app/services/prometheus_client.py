"""
Async Prometheus HTTP API client.
"""
import logging
from typing import Any, Dict, List, Optional
import httpx
from app.core.config import settings

logger = logging.getLogger(__name__)


class PrometheusClient:
    def __init__(self, base_url: Optional[str] = None):
        self._base = base_url or settings.PROMETHEUS_URL
        self._client = httpx.AsyncClient(
            base_url=self._base,
            timeout=30,
            verify=settings.VERIFY_TLS,
        )

    async def aclose(self):
        """Close the underlying HTTP client."""
        await self._client.aclose()

    async def instant_query(self, query: str, time: Optional[str] = None) -> List[dict]:
        """Execute an instant PromQL query."""
        params = {"query": query}
        if time:
            params["time"] = time
        try:
            resp = await self._client.get("/api/v1/query", params=params)
            resp.raise_for_status()
            data = resp.json()
            return data.get("data", {}).get("result", [])
        except httpx.ConnectError:
            logger.debug(f"Prometheus not reachable at {self._base}")
            return []
        except Exception as e:
            logger.debug(f"Prometheus query error: {e}")
            return []

    async def range_query(
        self, query: str, start: str, end: str, step: str = "60s"
    ) -> List[dict]:
        """Execute a range PromQL query."""
        params = {"query": query, "start": start, "end": end, "step": step}
        try:
            resp = await self._client.get("/api/v1/query_range", params=params)
            resp.raise_for_status()
            data = resp.json()
            return data.get("data", {}).get("result", [])
        except Exception as e:
            logger.debug(f"Prometheus range query error: {e}")
            return []

    async def get_active_alerts(self) -> List[dict]:
        """Fetch active alerts from Prometheus Alertmanager."""
        try:
            client = httpx.AsyncClient(base_url=settings.ALERTMANAGER_URL, timeout=15, verify=settings.VERIFY_TLS)
            resp = await client.get("/api/v2/alerts")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.debug(f"Alertmanager not reachable: {e}")
            return []

    async def check_health(self) -> bool:
        try:
            resp = await self._client.get("/-/healthy", timeout=5)
            return resp.status_code == 200
        except Exception:
            return False

    async def list_metrics(self) -> List[str]:
        try:
            resp = await self._client.get("/api/v1/label/__name__/values")
            resp.raise_for_status()
            return resp.json().get("data", [])
        except Exception:
            return []
