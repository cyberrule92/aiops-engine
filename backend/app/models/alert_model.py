"""
ORM models for alerts, anomalies, data sources.
"""
import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import String, Float, DateTime, JSON, Text, Boolean, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.core.database import Base


def gen_uuid() -> str:
    return str(uuid.uuid4())


class DataSource(Base):
    __tablename__ = "data_sources"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=gen_uuid)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    source_type: Mapped[str] = mapped_column(String(32))  # prometheus | otel
    endpoint: Mapped[str] = mapped_column(String(512))
    status: Mapped[str] = mapped_column(String(32), default="pending")  # ok | error | pending
    last_scrape: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    config: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)


class Alert(Base):
    __tablename__ = "alerts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=gen_uuid)
    fingerprint: Mapped[str] = mapped_column(String(128), index=True)
    source_type: Mapped[str] = mapped_column(String(32))  # prometheus | otel | manual
    source_id: Mapped[Optional[str]] = mapped_column(String(36), ForeignKey("data_sources.id"), nullable=True)
    name: Mapped[str] = mapped_column(String(256))
    severity: Mapped[str] = mapped_column(String(32))   # critical | warning | info
    status: Mapped[str] = mapped_column(String(32), default="firing")  # firing | resolved | suppressed
    namespace: Mapped[Optional[str]] = mapped_column(String(128))
    service: Mapped[Optional[str]] = mapped_column(String(128))
    node: Mapped[Optional[str]] = mapped_column(String(128))
    labels: Mapped[Optional[dict]] = mapped_column(JSON)
    annotations: Mapped[Optional[dict]] = mapped_column(JSON)
    raw_payload: Mapped[Optional[dict]] = mapped_column(JSON)
    starts_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    ends_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Correlation group
    correlation_group_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)

    # Anomaly link
    anomaly_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)


class Anomaly(Base):
    __tablename__ = "anomalies"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=gen_uuid)
    metric_name: Mapped[str] = mapped_column(String(256))
    labels: Mapped[Optional[dict]] = mapped_column(JSON)
    score: Mapped[float] = mapped_column(Float)          # 0-1
    zscore: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    method: Mapped[str] = mapped_column(String(64))      # isolation_forest | zscore | prophet | ensemble
    detected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    metric_value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    baseline_value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    deviation_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    namespace: Mapped[Optional[str]] = mapped_column(String(128))
    service: Mapped[Optional[str]] = mapped_column(String(128))
    node: Mapped[Optional[str]] = mapped_column(String(128))
    context: Mapped[Optional[dict]] = mapped_column(JSON)
    acknowledged: Mapped[bool] = mapped_column(Boolean, default=False)
    # Forecasting: "observed" (already happened) vs "predicted" (forecast breach).
    kind: Mapped[str] = mapped_column(String(16), default="observed", index=True)
    predicted_breach_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    prediction_status: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)  # pending|confirmed|expired


class CorrelationGroup(Base):
    __tablename__ = "correlation_groups"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=gen_uuid)
    root_alert_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    alert_ids: Mapped[Optional[list]] = mapped_column(JSON)      # list of alert IDs
    anomaly_ids: Mapped[Optional[list]] = mapped_column(JSON)
    similarity_score: Mapped[float] = mapped_column(Float, default=0.0)
    pattern: Mapped[Optional[str]] = mapped_column(String(256))  # e.g. "cascade", "fan-out", "oscillation"
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    window_start: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    window_end: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    labels: Mapped[Optional[dict]] = mapped_column(JSON)
    # Stable hash of the sorted member alert IDs — used to dedup groups so the
    # same cluster isn't re-created (and re-inferred) every correlation sweep.
    signature: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    # LLM-synthesized "inference alert" describing the correlated incident.
    inference: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    inference_status: Mapped[str] = mapped_column(String(32), default="pending")  # pending|completed|failed
    # Topology-derived causal root entity key (e.g. "node:10.0.0.1") + the
    # dependency subgraph used to reason about this group.
    root_entity: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    topology: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)


class RCAReport(Base):
    __tablename__ = "rca_reports"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=gen_uuid)
    correlation_group_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    alert_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    anomaly_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    root_cause: Mapped[str] = mapped_column(Text)              # == probable_root_cause (legacy alias)
    contributing_factors: Mapped[Optional[list]] = mapped_column(JSON)
    remediation_steps: Mapped[Optional[list]] = mapped_column(JSON)  # legacy flattened short+long
    confidence: Mapped[float] = mapped_column(Float)            # 0-1
    llm_model: Mapped[str] = mapped_column(String(64))
    llm_prompt_tokens: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    raw_llm_response: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="completed")

    # --- Rich investigative RCA (chain-of-thought) -------------------------
    problem_overview: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    impact: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    timeline: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)            # [{time,event,type}]
    supporting_evidence: Mapped[Optional[list]] = mapped_column(JSON, nullable=True) # [{observation,implication}]
    hypotheses: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)          # [{hypothesis,verdict,evidence_for,evidence_against,reasoning}]
    reasoning: Mapped[Optional[str]] = mapped_column(Text, nullable=True)            # chain-of-thought narrative
    short_term_actions: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)  # [{action,rationale,command}]
    long_term_actions: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)   # [{action,rationale}]
    related_components: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)


class TopologyNode(Base):
    """An entity in the service dependency graph (service, node, namespace, …)."""
    __tablename__ = "topology_nodes"

    entity_key: Mapped[str] = mapped_column(String(256), primary_key=True)  # "type:name"
    entity_type: Mapped[str] = mapped_column(String(32), index=True)        # service|node|namespace|instance|pod|job
    name: Mapped[str] = mapped_column(String(256))
    labels: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    alert_count: Mapped[int] = mapped_column(Integer, default=0)
    first_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class TopologyEdge(Base):
    """A directed dependency edge src → dst (src affects/causes dst)."""
    __tablename__ = "topology_edges"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=gen_uuid)
    src_key: Mapped[str] = mapped_column(String(256), index=True)
    dst_key: Mapped[str] = mapped_column(String(256), index=True)
    edge_type: Mapped[str] = mapped_column(String(16), default="temporal")  # structural|temporal|declared
    relation: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)  # hosts|manages|contains|connects|depends_on
    weight: Mapped[float] = mapped_column(Float, default=1.0)
    confidence: Mapped[float] = mapped_column(Float, default=0.5)
    occurrences: Mapped[int] = mapped_column(Integer, default=1)
    first_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
