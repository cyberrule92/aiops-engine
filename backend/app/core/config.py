"""
Application configuration via environment variables.
"""
from typing import List, Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # App
    APP_NAME: str = "AIOps Intelligence Engine"
    DEBUG: bool = False
    SECRET_KEY: str = "change-me-in-production"

    # Database (SQLite for portability; swap to Postgres in prod)
    DATABASE_URL: str = "sqlite+aiosqlite:////data/aiops.db"

    # Prometheus
    PROMETHEUS_URL: str = "http://prometheus:9090"
    PROMETHEUS_SCRAPE_INTERVAL: int = 30  # seconds
    ALERTMANAGER_URL: str = "http://alertmanager:9093"

    # TLS verification for outbound HTTP (Prometheus / Alertmanager / sources).
    # Set false for self-signed or internal-CA endpoints. Prefer mounting a CA
    # bundle in production rather than disabling verification globally.
    VERIFY_TLS: bool = True

    # OTEL Collector
    OTEL_GRPC_ENDPOINT: str = "http://otel-collector:4317"
    OTEL_HTTP_ENDPOINT: str = "http://otel-collector:4318"

    # Ollama LLM
    OLLAMA_BASE_URL: str = "http://ollama:11434"
    OLLAMA_MODEL: str = "llama3"
    # Investigative RCA produces a large structured JSON (timeline + hypotheses +
    # evidence + short/long-term actions). On CPU-only Ollama that generation can
    # run several minutes, so the read timeout must be generous.
    OLLAMA_TIMEOUT: int = 360

    # ML / Anomaly Detection
    ANOMALY_DETECTION_INTERVAL: int = 60       # seconds
    CORRELATION_WINDOW_SECONDS: int = 300      # 5 min window
    ZSCORE_THRESHOLD: float = 3.0
    ISOLATION_FOREST_CONTAMINATION: float = 0.05
    MIN_ANOMALY_SCORE: float = 0.6

    # Per-series anti-spam for OBSERVED anomalies. A series that stays anomalous
    # would otherwise emit a fresh anomaly every detection sweep; after it fires,
    # suppress re-firing the SAME series for this many seconds — unless its score
    # escalates by ANOMALY_REFIRE_SCORE_DELTA (so a worsening incident still shows).
    ANOMALY_COOLDOWN_SECONDS: int = 900       # 15 min
    ANOMALY_REFIRE_SCORE_DELTA: float = 0.15  # re-fire if score jumps this much

    # Predictive / forecasting (feature: preventive ops)
    FORECAST_ENABLED: bool = True
    FORECAST_HORIZON_SECONDS: int = 1800       # how far ahead to project (30 min)
    FORECAST_MIN_POINTS: int = 30              # min samples before forecasting
    FORECAST_MIN_CONFIDENCE: float = 0.55      # suppress low-confidence predictions
    FORECAST_MAX_FIT_ERROR: float = 0.40       # max in-sample MAPE; above = unreliable, skip
    FORECAST_COOLDOWN_SECONDS: int = 1800      # don't re-predict same series within this
    FORECAST_STEP_SECONDS: int = 30            # assumed sample spacing for horizon→steps

    # Topology / causal correlation (feature: dependency-aware RCA)
    TOPOLOGY_ENABLED: bool = True
    TOPOLOGY_EDGE_DECAY: float = 0.98          # multiplicative decay applied each sweep
    TOPOLOGY_MIN_EDGE_WEIGHT: float = 0.15     # prune edges weaker than this
    TOPOLOGY_EDGE_TTL_SECONDS: int = 604800    # drop edges unseen for 7 days
    TOPOLOGY_MAX_NODES: int = 2000             # safety cap on graph size

    # CORS
    CORS_ORIGINS: List[str] = ["*"]

    # Alerting thresholds
    ALERT_DEDUP_WINDOW: int = 60               # seconds
    MAX_ALERTS_MEMORY: int = 10000

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
