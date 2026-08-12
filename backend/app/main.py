"""
AIOps Anomaly Detection & RCA Engine
Production-grade FastAPI application with Prometheus/OTEL ingestion,
ML-based anomaly detection, alert correlation, and Ollama-powered RCA.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles

from app.api import alerts, anomalies, correlation, rca, sources, health, metrics_proxy, topology
from app.core.config import settings
from app.core.database import init_db
from app.core.event_bus import event_bus
from app.services.ingestion_service import IngestionService
from app.services.anomaly_service import AnomalyService
from app.services.correlation_service import AlertCorrelationService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan — startup/shutdown."""
    logger.info("🚀 AIOps Engine starting up...")
    await init_db()

    # Start background services
    ingestion = IngestionService()
    anomaly_svc = AnomalyService()
    correlation_svc = AlertCorrelationService()

    tasks = [
        asyncio.create_task(ingestion.start_prometheus_scraper(), name="prometheus-scraper"),
        asyncio.create_task(anomaly_svc.start_detection_loop(), name="anomaly-detector"),
        asyncio.create_task(correlation_svc.start_correlation_loop(), name="correlator"),
    ]

    logger.info("✅ All background services started")
    yield

    # Graceful shutdown — BOUNDED so a wedged loop (e.g. mid multi-minute Ollama
    # call or a large CPU detection sweep) can never hang the process. We cancel
    # every task, then wait at most a few seconds; whatever hasn't stopped is
    # abandoned as the event loop tears down. This is what keeps `systemctl
    # restart` fast and clean instead of timing out into SIGKILL.
    logger.info("🛑 Shutting down background services...")
    for task in tasks:
        task.cancel()
    try:
        _, pending = await asyncio.wait(tasks, timeout=8)
        if pending:
            logger.warning(f"⚠️  {len(pending)} background task(s) did not stop in time; abandoning")
    except Exception as e:  # never let shutdown raise
        logger.warning(f"Shutdown wait error (ignored): {e}")
    logger.info("✅ Shutdown complete")


app = FastAPI(
    title="AIOps Intelligence Engine",
    description="AI-powered anomaly detection, alert correlation, and RCA for Kubernetes environments",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

# Middleware
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(health.router, prefix="/api/v1", tags=["Health"])
app.include_router(sources.router, prefix="/api/v1", tags=["Data Sources"])
app.include_router(alerts.router, prefix="/api/v1", tags=["Alerts"])
app.include_router(anomalies.router, prefix="/api/v1", tags=["Anomalies"])
app.include_router(correlation.router, prefix="/api/v1", tags=["Correlation"])
app.include_router(rca.router, prefix="/api/v1", tags=["RCA"])
app.include_router(metrics_proxy.router, prefix="/api/v1", tags=["Metrics Proxy"])
app.include_router(topology.router, prefix="/api/v1", tags=["Topology"])

# Serve the frontend from the same origin as the API so the browser never needs
# a hard-coded backend host. Mounted last so /api/* and /api/docs win. The UI's
# API base is '' (relative), so it always targets whatever host served the page.
FRONTEND_DIR = os.environ.get("FRONTEND_DIR", "/opt/aiops-engine/frontend")
if os.path.isdir(FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")
    logger.info(f"📊 Serving frontend from {FRONTEND_DIR} at /")
