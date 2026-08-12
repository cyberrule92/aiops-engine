"""
Async SQLAlchemy database setup.
"""
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    pool_pre_ping=True,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


# Lightweight additive migrations for SQLite (create_all never ALTERs existing
# tables). Each entry: (table, column, column definition). Re-running is safe.
_MIGRATIONS = [
    ("correlation_groups", "signature", "VARCHAR(64)"),
    ("correlation_groups", "inference", "JSON"),
    ("correlation_groups", "inference_status", "VARCHAR(32) DEFAULT 'pending'"),
    ("correlation_groups", "root_entity", "VARCHAR(256)"),
    ("correlation_groups", "topology", "JSON"),
    # Predictive anomalies (forecasting). Forecast specifics live in context JSON.
    ("anomalies", "kind", "VARCHAR(16) DEFAULT 'observed'"),
    ("anomalies", "predicted_breach_at", "DATETIME"),
    ("anomalies", "prediction_status", "VARCHAR(16)"),
    ("topology_edges", "relation", "VARCHAR(32)"),
    # Rich investigative RCA (chain-of-thought) fields.
    ("rca_reports", "problem_overview", "TEXT"),
    ("rca_reports", "impact", "TEXT"),
    ("rca_reports", "timeline", "JSON"),
    ("rca_reports", "supporting_evidence", "JSON"),
    ("rca_reports", "hypotheses", "JSON"),
    ("rca_reports", "reasoning", "TEXT"),
    ("rca_reports", "short_term_actions", "JSON"),
    ("rca_reports", "long_term_actions", "JSON"),
    ("rca_reports", "related_components", "JSON"),
]


async def _apply_migrations(conn):
    from sqlalchemy import text
    for table, column, coldef in _MIGRATIONS:
        rows = await conn.execute(text(f"PRAGMA table_info({table})"))
        existing = {r[1] for r in rows.fetchall()}
        if column not in existing:
            await conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {coldef}"))
            logger.info(f"🔧 Migration: added {table}.{column}")


async def init_db():
    """Create all tables, then apply additive column migrations."""
    from app.models import alert_model, anomaly_model, source_model  # noqa
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await _apply_migrations(conn)
    logger.info("✅ Database initialized")


async def get_db():
    """FastAPI dependency: get async DB session."""
    async with AsyncSessionLocal() as session:
        yield session
