"""
ML Anomaly Detection Service.
Implements ensemble of:
  1. Z-Score (fast, stateless)
  2. Isolation Forest (unsupervised, sklearn)
  3. EWMA (exponentially-weighted moving average drift detector)
Runs continuously on metrics scraped from Prometheus.
"""

import asyncio
import logging
import math
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sqlalchemy import select, update

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.core.event_bus import event_bus
from app.models.alert_model import Anomaly, DataSource
from app.services.prometheus_client import PrometheusClient

logger = logging.getLogger(__name__)


class MetricWindow:
    """Rolling window of metric values for a single time-series."""

    def __init__(self, maxlen: int = 360):
        self.values: deque = deque(maxlen=maxlen)   # 360 * 30s = 3h window
        self.timestamps: deque = deque(maxlen=maxlen)
        self.labels: dict = {}
        self._scaler = StandardScaler()
        self._iso_forest: Optional[IsolationForest] = None
        self._iso_trained_at: float = 0
        self._retrain_interval: int = 300  # retrain every 5 min

    def push(self, value: float, ts: float):
        self.values.append(value)
        self.timestamps.append(ts)

    def needs_retrain(self) -> bool:
        return (
            len(self.values) >= 30 and
            time.time() - self._iso_trained_at > self._retrain_interval
        )

    def train_iso(self):
        X = np.array(list(self.values)).reshape(-1, 1)
        X_scaled = self._scaler.fit_transform(X)
        self._iso_forest = IsolationForest(
            contamination=settings.ISOLATION_FOREST_CONTAMINATION,
            n_estimators=100,
            random_state=42,
            n_jobs=-1,
        )
        self._iso_forest.fit(X_scaled)
        self._iso_trained_at = time.time()

    def zscore(self, value: float) -> float:
        if len(self.values) < 10:
            return 0.0
        arr = np.array(list(self.values))
        mean, std = arr.mean(), arr.std()
        if std == 0:
            return 0.0
        return abs((value - mean) / std)

    def iso_score(self, value: float) -> float:
        """Returns anomaly score 0-1 (1 = most anomalous)."""
        if self._iso_forest is None or len(self.values) < 30:
            return 0.0
        X = np.array([[value]])
        X_scaled = self._scaler.transform(X)
        # decision_function returns negative scores; normalize to 0-1
        raw = self._iso_forest.decision_function(X_scaled)[0]
        score = 1 / (1 + np.exp(raw * 5))
        return float(score)

    def ewma_deviation(self, value: float, alpha: float = 0.1) -> float:
        """EWMA drift: deviation of current value from EWMA baseline."""
        if len(self.values) < 5:
            return 0.0
        arr = list(self.values)
        ewma = arr[0]
        for v in arr[1:]:
            ewma = alpha * v + (1 - alpha) * ewma
        if ewma == 0:
            return 0.0
        return abs((value - ewma) / ewma)

    def ensemble_score(self, value: float) -> Tuple[float, dict]:
        """Weighted ensemble of all detectors. Returns score 0-1."""
        z = self.zscore(value)
        iso = self.iso_score(value)
        ewma = self.ewma_deviation(value)

        # Normalize z-score to 0-1
        z_norm = min(z / settings.ZSCORE_THRESHOLD, 1.0)

        # Weights: IsoForest 50%, ZScore 30%, EWMA 20%
        score = 0.5 * iso + 0.3 * z_norm + 0.2 * min(ewma, 1.0)

        details = {
            "isolation_forest": round(iso, 4),
            "zscore": round(z, 4),
            "ewma_deviation": round(ewma, 4),
            "ensemble": round(score, 4),
        }
        return score, details

    def forecast(self, horizon_steps: int) -> Optional[dict]:
        """Holt damped-trend forecast over the window.

        Returns {level, trend, resid_std, mape, points:[(yhat, lo, hi)]} or None
        when there isn't enough clean data. Damped trend avoids the runaway
        extrapolation of pure linear trend on noisy infra metrics.
        """
        if len(self.values) < settings.FORECAST_MIN_POINTS:
            return None
        y = np.asarray(list(self.values), dtype=float)
        y = y[np.isfinite(y)]
        if y.size < settings.FORECAST_MIN_POINTS:
            return None

        alpha, beta, phi = 0.3, 0.1, 0.95
        level = float(y[0])
        trend = float(y[1] - y[0])
        onestep_err, abs_pct_err = [], []
        for t in range(1, y.size):
            fcast = level + phi * trend           # one-step-ahead for y[t]
            err = float(y[t] - fcast)
            onestep_err.append(err)
            denom = abs(float(y[t])) if y[t] != 0 else 1e-9
            abs_pct_err.append(abs(err) / denom)
            prev_level = level
            level = alpha * float(y[t]) + (1 - alpha) * (level + phi * trend)
            trend = beta * (level - prev_level) + (1 - beta) * phi * trend

        if not (math.isfinite(level) and math.isfinite(trend)):
            return None
        resid_std = float(np.std(onestep_err)) if onestep_err else 0.0
        mape = float(np.mean(abs_pct_err)) if abs_pct_err else 1.0

        points = []
        cum, phi_pow = 0.0, 1.0
        for h in range(1, horizon_steps + 1):
            phi_pow *= phi
            cum += phi_pow
            point = level + cum * trend
            band = 2.0 * resid_std * math.sqrt(h)   # ~95% interval, widening with h
            points.append((point, point - band, point + band))
        return {"level": level, "trend": trend, "resid_std": resid_std,
                "mape": mape, "points": points}


class AnomalyService:
    def __init__(self):
        self._windows: Dict[str, MetricWindow] = defaultdict(MetricWindow)
        # Per-endpoint Prometheus clients, built lazily from configured sources.
        self._clients: Dict[str, PrometheusClient] = {}
        self._recent_anomalies: deque = deque(maxlen=1000)
        # series_key -> epoch until which we won't re-predict (anti-spam).
        self._predict_cooldown: Dict[str, float] = {}
        # series_key -> epoch until which we won't re-emit an OBSERVED anomaly,
        # plus the last emitted score (for the escalation override).
        self._observed_cooldown: Dict[str, float] = {}
        self._observed_last_score: Dict[str, float] = {}
        self._last_reconcile: float = 0.0

    def _series_key(self, metric_name: str, labels: dict, source: str = "") -> str:
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        prefix = f"{source}/" if source else ""
        return f"{prefix}{metric_name}{{{label_str}}}"

    async def analyze_metric(
        self,
        metric_name: str,
        value: float,
        labels: dict,
        ts: Optional[float] = None,
        source: str = "",
    ) -> Optional[dict]:
        """Analyze a single metric value. Returns anomaly dict if detected."""
        ts = ts or time.time()
        key = self._series_key(metric_name, labels, source)
        window = self._windows[key]
        window.labels = labels

        if window.needs_retrain():
            window.train_iso()

        score, details = window.ensemble_score(value)
        window.push(value, ts)

        if score >= settings.MIN_ANOMALY_SCORE:
            # Per-series cooldown/dedup: a persistently-anomalous series would
            # otherwise emit a fresh anomaly every sweep. Suppress repeats inside
            # the cooldown window, but still re-fire if the score escalates
            # meaningfully so a worsening incident isn't masked.
            now = time.time()
            prev_score = self._observed_last_score.get(key)
            cooling = self._observed_cooldown.get(key, 0.0) > now
            escalated = (
                prev_score is not None
                and (score - prev_score) >= settings.ANOMALY_REFIRE_SCORE_DELTA
            )
            if cooling and not escalated:
                return None  # deduped — same series already reported recently
            self._observed_cooldown[key] = now + settings.ANOMALY_COOLDOWN_SECONDS
            self._observed_last_score[key] = score

            arr = np.array(list(window.values)) if window.values else np.array([value])
            baseline = float(arr.mean()) if len(arr) > 1 else value
            deviation = ((value - baseline) / baseline * 100) if baseline != 0 else 0

            anomaly = {
                "metric_name": metric_name,
                "labels": labels,
                "score": round(score, 4),
                "zscore": round(details["zscore"], 4),
                "method": "ensemble",
                "metric_value": round(value, 6),
                "baseline_value": round(baseline, 6),
                "deviation_pct": round(deviation, 2),
                "namespace": labels.get("namespace"),
                "service": labels.get("service", labels.get("job")),
                "node": labels.get("node", labels.get("instance")),
                "context": details,
                "detected_at": datetime.utcnow().isoformat(),
            }
            return anomaly
        return None

    @staticmethod
    def _parse_dt(v):
        if isinstance(v, datetime) or v is None:
            return v
        try:
            return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None

    async def _persist_anomaly(self, anomaly: dict) -> str:
        # Only pass columns the model actually defines; the anomaly dict carries
        # extra transport-only keys (e.g. source_id, forecast_value, detected_at, id).
        cols = {c.name for c in Anomaly.__table__.columns}
        fields = {k: v for k, v in anomaly.items() if k in cols and k not in ("id", "detected_at")}
        # DateTime columns must receive datetime objects, not ISO strings.
        if "predicted_breach_at" in fields:
            fields["predicted_breach_at"] = self._parse_dt(fields["predicted_breach_at"])
        async with AsyncSessionLocal() as db:
            obj = Anomaly(**fields)
            obj.detected_at = datetime.utcnow()
            db.add(obj)
            await db.commit()
            await db.refresh(obj)
            return obj.id

    def predict_metric(
        self, metric_name: str, value: float, labels: dict, source: str = ""
    ) -> Optional[dict]:
        """Forecast the series and, if it's trending toward an anomalous band,
        return a *predicted* anomaly dict (with ETA). Returns None otherwise.

        Guards: forecasting disabled, cold start, flat/degenerate series, poor
        model fit, low confidence, and a per-series cooldown to avoid spam.
        """
        if not settings.FORECAST_ENABLED:
            return None
        key = self._series_key(metric_name, labels, source)
        window = self._windows.get(key)
        if window is None or len(window.values) < settings.FORECAST_MIN_POINTS:
            return None

        now = time.time()
        if self._predict_cooldown.get(key, 0.0) > now:
            return None

        arr = np.asarray(list(window.values), dtype=float)
        arr = arr[np.isfinite(arr)]
        if arr.size < settings.FORECAST_MIN_POINTS:
            return None
        mean, std = float(arr.mean()), float(arr.std())
        if std <= 0 or not math.isfinite(std):
            return None  # flat series: nothing meaningful to forecast

        step = max(1, int(settings.FORECAST_STEP_SECONDS))
        horizon_steps = max(1, int(settings.FORECAST_HORIZON_SECONDS // step))
        fc = window.forecast(horizon_steps)
        if not fc or fc["mape"] > settings.FORECAST_MAX_FIT_ERROR:
            return None  # unreliable fit → don't cry wolf

        thresh = settings.ZSCORE_THRESHOLD
        # Already anomalous now? That's the observed detector's job, not forecasting.
        if abs(value - mean) / std >= thresh:
            return None

        breach_step = breach_val = None
        for i, (point, _lo, _hi) in enumerate(fc["points"], start=1):
            if abs(point - mean) / std >= thresh:
                breach_step, breach_val = i, point
                break
        if breach_step is None:
            return None

        z_at_breach = abs(breach_val - mean) / std
        fit_quality = max(0.0, 1.0 - fc["mape"] / max(settings.FORECAST_MAX_FIT_ERROR, 1e-6))
        decisiveness = min(1.0, (z_at_breach - thresh) / thresh + 0.5)
        confidence = round(0.5 * fit_quality + 0.5 * decisiveness, 4)
        if confidence < settings.FORECAST_MIN_CONFIDENCE:
            return None

        self._predict_cooldown[key] = now + settings.FORECAST_COOLDOWN_SECONDS
        ttb = breach_step * step
        breach_at = datetime.utcnow() + timedelta(seconds=ttb)
        deviation = ((breach_val - mean) / mean * 100) if mean != 0 else 0.0
        direction = "increase" if breach_val > mean else "decrease"
        return {
            "metric_name": metric_name,
            "labels": labels,
            "score": confidence,
            "zscore": round(z_at_breach, 4),
            "method": "forecast",
            "kind": "predicted",
            "prediction_status": "pending",
            "metric_value": round(value, 6),
            "baseline_value": round(mean, 6),
            "deviation_pct": round(deviation, 2),
            "namespace": labels.get("namespace"),
            "service": labels.get("service", labels.get("job")),
            "node": labels.get("node", labels.get("instance")),
            "predicted_breach_at": breach_at.isoformat(),
            "forecast_value": round(breach_val, 6),  # transport-only; also in context
            "context": {
                "forecast": True,
                "direction": direction,
                "forecast_value": round(breach_val, 6),
                "horizon_seconds": settings.FORECAST_HORIZON_SECONDS,
                "time_to_breach_seconds": ttb,
                "mape": round(fc["mape"], 4),
                "trend_per_step": round(fc["trend"], 6),
                "resid_std": round(fc["resid_std"], 6),
                "zscore_threshold": thresh,
            },
            "detected_at": datetime.utcnow().isoformat(),
        }

    async def _reconcile_predictions(self):
        """Resolve past predictions: confirmed if a matching observed anomaly
        landed in the window, else expired. Keeps the predicted list honest."""
        grace = max(2 * settings.ANOMALY_DETECTION_INTERVAL, 300)
        cutoff = datetime.utcnow() - timedelta(seconds=grace)
        async with AsyncSessionLocal() as db:
            due = (await db.execute(
                select(Anomaly).where(
                    Anomaly.kind == "predicted",
                    Anomaly.prediction_status == "pending",
                    Anomaly.predicted_breach_at.isnot(None),
                    Anomaly.predicted_breach_at < cutoff,
                )
            )).scalars().all()
            for p in due:
                hi = (p.predicted_breach_at or p.detected_at) + timedelta(seconds=grace)
                observed = (await db.execute(
                    select(Anomaly.id).where(
                        Anomaly.kind == "observed",
                        Anomaly.metric_name == p.metric_name,
                        Anomaly.service == p.service,
                        Anomaly.namespace == p.namespace,
                        Anomaly.detected_at >= p.detected_at,
                        Anomaly.detected_at <= hi,
                    ).limit(1)
                )).scalars().first()
                p.prediction_status = "confirmed" if observed else "expired"
            if due:
                await db.commit()
                logger.info(f"🔮 Reconciled {len(due)} prediction(s)")

    # Core metrics to watch on every Prometheus source.
    METRIC_QUERIES = [
        "rate(http_requests_total[5m])",
        "histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))",
        "rate(http_requests_total{status=~'5..'}[5m])",
        "container_cpu_usage_seconds_total",
        "container_memory_working_set_bytes",
        "kube_pod_container_status_restarts_total",
        "node_cpu_seconds_total{mode='idle'}",
        "node_memory_MemAvailable_bytes",
    ]

    async def _resolve_targets(self) -> List[Tuple[str, str]]:
        """Return [(source_id, endpoint)] for configured Prometheus sources.

        Falls back to the PROMETHEUS_URL env var when no sources are configured,
        so env-driven deployments keep working unchanged.
        """
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(DataSource).where(DataSource.source_type == "prometheus")
            )
            sources = result.scalars().all()
        targets = [(s.id, s.endpoint) for s in sources if s.endpoint]
        if not targets and settings.PROMETHEUS_URL:
            targets = [("__env__", settings.PROMETHEUS_URL)]
        return targets

    def _client_for(self, endpoint: str) -> PrometheusClient:
        client = self._clients.get(endpoint)
        if client is None:
            client = PrometheusClient(base_url=endpoint)
            self._clients[endpoint] = client
        return client

    async def _mark_scraped(self, source_id: str):
        """Stamp last_scrape on a DataSource so the UI reflects live activity."""
        if source_id == "__env__":
            return
        try:
            async with AsyncSessionLocal() as db:
                src = await db.get(DataSource, source_id)
                if src:
                    src.last_scrape = datetime.utcnow()
                    await db.commit()
        except Exception as e:
            logger.debug(f"Could not update last_scrape for {source_id}: {e}")

    async def _scrape_target(self, source_id: str, endpoint: str):
        """Run all metric queries against a single Prometheus source."""
        client = self._client_for(endpoint)
        scraped_any = False
        for query in self.METRIC_QUERIES:
            try:
                results = await client.instant_query(query)
                if results:
                    scraped_any = True
                for result in results:
                    metric = result.get("metric", {})
                    value_raw = result.get("value", [None, None])
                    if value_raw[1] is None:
                        continue
                    try:
                        value = float(value_raw[1])
                    except (ValueError, TypeError):
                        continue

                    metric_name = metric.get("__name__", query[:64])
                    labels = {k: v for k, v in metric.items() if k != "__name__"}

                    anomaly = await self.analyze_metric(
                        metric_name, value, labels, source=source_id
                    )
                    if anomaly:
                        anomaly["source_id"] = source_id
                        anomaly_id = await self._persist_anomaly(anomaly)
                        anomaly["id"] = anomaly_id
                        self._recent_anomalies.append(anomaly)
                        await event_bus.publish("anomalies", anomaly)
                        logger.info(
                            f"🔴 Anomaly detected: {metric_name} score={anomaly['score']} "
                            f"src={source_id}"
                        )
                    else:
                        # Not anomalous now — is it trending toward a breach?
                        predicted = self.predict_metric(
                            metric_name, value, labels, source=source_id
                        )
                        if predicted:
                            predicted["source_id"] = source_id
                            pid = await self._persist_anomaly(predicted)
                            predicted["id"] = pid
                            self._recent_anomalies.append(predicted)
                            await event_bus.publish("anomalies", predicted)
                            ttb = predicted["context"]["time_to_breach_seconds"]
                            logger.info(
                                f"🔮 Predicted anomaly: {metric_name} in ~{ttb}s "
                                f"conf={predicted['score']} src={source_id}"
                            )
            except Exception as e:
                logger.debug(f"Query failed ({query}) on {endpoint}: {e}")
        if scraped_any:
            await self._mark_scraped(source_id)

    async def _scrape_and_detect(self):
        """Scrape key metrics from every configured Prometheus source."""
        targets = await self._resolve_targets()

        # Drop cached clients for endpoints that are no longer configured.
        active = {ep for _, ep in targets}
        for ep in list(self._clients):
            if ep not in active:
                try:
                    await self._clients.pop(ep).aclose()
                except Exception:
                    self._clients.pop(ep, None)

        for source_id, endpoint in targets:
            await self._scrape_target(source_id, endpoint)

        # Periodically resolve past predictions (confirmed vs expired).
        now = time.time()
        if settings.FORECAST_ENABLED and now - self._last_reconcile > max(
            settings.ANOMALY_DETECTION_INTERVAL, 60
        ):
            self._last_reconcile = now
            try:
                await self._reconcile_predictions()
            except Exception as e:
                logger.debug(f"Prediction reconcile error: {e}")

    async def start_detection_loop(self):
        """Main detection loop."""
        logger.info("🤖 Anomaly detection loop started")
        while True:
            try:
                await self._scrape_and_detect()
            except Exception as e:
                logger.error(f"Detection loop error: {e}")
            await asyncio.sleep(settings.ANOMALY_DETECTION_INTERVAL)

    def get_recent_anomalies(self, limit: int = 100) -> List[dict]:
        items = list(self._recent_anomalies)
        return items[-limit:]


# Singleton
_anomaly_service: Optional[AnomalyService] = None


def get_anomaly_service() -> AnomalyService:
    global _anomaly_service
    if _anomaly_service is None:
        _anomaly_service = AnomalyService()
    return _anomaly_service
