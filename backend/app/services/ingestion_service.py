"""
Ingestion Service.
- Scrapes Prometheus Alertmanager for active alerts
- Receives Alertmanager webhook pushes (via /api/v1/alerts/webhook)
- Receives OTEL OTLP HTTP log/span events via webhook
Normalizes all events into the unified Alert model.
"""

import asyncio
import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.core.event_bus import event_bus
from app.models.alert_model import Alert
from app.services.prometheus_client import PrometheusClient

logger = logging.getLogger(__name__)


def _fingerprint(alert_dict: dict) -> str:
    """Stable fingerprint for dedup.

    Includes starts_at so the same ongoing alert occurrence maps to one row
    (Alertmanager re-sends it every poll with the same startsAt), while a genuine
    new occurrence gets a new fingerprint. This makes dedup stable across restarts.
    """
    key = json.dumps(
        {
            "name": alert_dict.get("name"),
            "namespace": alert_dict.get("namespace"),
            "service": alert_dict.get("service"),
            "starts_at": alert_dict.get("starts_at"),
        },
        sort_keys=True,
    )
    return hashlib.sha256(key.encode()).hexdigest()[:32]


def normalize_prometheus_alert(raw: dict) -> dict:
    """Normalize Alertmanager alert payload to internal format."""
    labels = raw.get("labels", {})
    annotations = raw.get("annotations", {})
    return {
        "source_type": "prometheus",
        "name": labels.get("alertname", "UnknownAlert"),
        "severity": labels.get("severity", "warning"),
        "namespace": labels.get("namespace"),
        "service": labels.get("service") or labels.get("job"),
        "node": labels.get("node") or labels.get("instance"),
        "labels": labels,
        "annotations": annotations,
        "status": "firing" if raw.get("status") == "firing" else "resolved",
        "starts_at": raw.get("startsAt"),
        "ends_at": raw.get("endsAt"),
        "raw_payload": raw,
    }


def normalize_otel_log(raw: dict) -> Optional[dict]:
    """Normalize OTLP log record to internal alert format."""
    # OTLP log records have severity number/text
    severity_map = {
        "ERROR": "critical",
        "WARN": "warning",
        "WARNING": "warning",
        "INFO": "info",
        "DEBUG": "info",
    }
    severity_text = raw.get("severityText", "INFO").upper()
    severity = severity_map.get(severity_text, "info")

    if severity == "info":
        return None  # Skip INFO logs for alert ingestion

    attrs = raw.get("attributes", {})
    resource = raw.get("resource", {}).get("attributes", {})

    return {
        "source_type": "otel",
        "name": attrs.get("event.name") or raw.get("body", "OTELAlert")[:128],
        "severity": severity,
        "namespace": resource.get("k8s.namespace.name") or attrs.get("namespace"),
        "service": resource.get("service.name"),
        "node": resource.get("k8s.node.name"),
        "labels": {**resource, **attrs},
        "annotations": {"body": raw.get("body", "")},
        "status": "firing",
        "starts_at": datetime.utcnow().isoformat(),
        "ends_at": None,
        "raw_payload": raw,
    }


class IngestionService:
    def __init__(self):
        self._prom = PrometheusClient()
        self._seen_fingerprints: set = set()

    async def ingest_alert(self, alert_dict: dict) -> Optional[str]:
        """Persist a normalized alert. Returns alert ID or None if deduped."""
        fp = _fingerprint(alert_dict)
        if fp in self._seen_fingerprints:
            return None
        # Persistent dedup: survive restarts (the in-memory set is empty on boot,
        # so without this the same active alerts get re-ingested as new rows).
        async with AsyncSessionLocal() as db:
            from sqlalchemy import select
            exists = await db.execute(
                select(Alert.id).where(Alert.fingerprint == fp).limit(1)
            )
            if exists.scalars().first():
                self._seen_fingerprints.add(fp)
                return None
        self._seen_fingerprints.add(fp)
        if len(self._seen_fingerprints) > settings.MAX_ALERTS_MEMORY:
            # Rolling eviction (keep recent half)
            evict = list(self._seen_fingerprints)[: settings.MAX_ALERTS_MEMORY // 2]
            for e in evict:
                self._seen_fingerprints.discard(e)

        # Parse starts_at
        starts_at = None
        if alert_dict.get("starts_at"):
            try:
                starts_at = datetime.fromisoformat(
                    alert_dict["starts_at"].replace("Z", "+00:00")
                )
            except Exception:
                starts_at = datetime.utcnow()

        async with AsyncSessionLocal() as db:
            obj = Alert(
                fingerprint=fp,
                source_type=alert_dict.get("source_type", "unknown"),
                name=alert_dict.get("name", "UnknownAlert"),
                severity=alert_dict.get("severity", "warning"),
                status=alert_dict.get("status", "firing"),
                namespace=alert_dict.get("namespace"),
                service=alert_dict.get("service"),
                node=alert_dict.get("node"),
                labels=alert_dict.get("labels"),
                annotations=alert_dict.get("annotations"),
                raw_payload=alert_dict.get("raw_payload"),
                starts_at=starts_at or datetime.utcnow(),
            )
            db.add(obj)
            await db.commit()
            await db.refresh(obj)

        alert_dict["id"] = obj.id
        await event_bus.publish("alerts", alert_dict)
        logger.info(
            f"📥 Alert ingested: [{alert_dict['severity'].upper()}] {alert_dict['name']} "
            f"ns={alert_dict.get('namespace')} svc={alert_dict.get('service')}"
        )
        return obj.id

    async def ingest_alertmanager_payload(self, payload: dict) -> List[str]:
        """Process Alertmanager webhook payload (contains list of alerts)."""
        ids = []
        alerts = payload.get("alerts", [])
        for raw in alerts:
            normalized = normalize_prometheus_alert(raw)
            aid = await self.ingest_alert(normalized)
            if aid:
                ids.append(aid)
        return ids

    async def ingest_otel_payload(self, payload: dict) -> List[str]:
        """Process OTLP/HTTP log payload."""
        ids = []
        for resource_log in payload.get("resourceLogs", []):
            resource = resource_log.get("resource", {})
            for scope_log in resource_log.get("scopeLogs", []):
                for log_record in scope_log.get("logRecords", []):
                    log_record["resource"] = resource
                    normalized = normalize_otel_log(log_record)
                    if normalized:
                        aid = await self.ingest_alert(normalized)
                        if aid:
                            ids.append(aid)
        return ids

    async def start_prometheus_scraper(self):
        """Poll Alertmanager for active alerts."""
        logger.info("⚙️  Prometheus scraper started")
        while True:
            try:
                raw_alerts = await self._prom.get_active_alerts()
                for raw in raw_alerts:
                    normalized = normalize_prometheus_alert(raw)
                    await self.ingest_alert(normalized)
            except Exception as e:
                logger.debug(f"Prometheus scraper error: {e}")
            await asyncio.sleep(settings.PROMETHEUS_SCRAPE_INTERVAL)


_ingestion_service: Optional[IngestionService] = None


def get_ingestion_service() -> IngestionService:
    global _ingestion_service
    if _ingestion_service is None:
        _ingestion_service = IngestionService()
    return _ingestion_service
