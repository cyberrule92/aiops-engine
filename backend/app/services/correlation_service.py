"""
Alert Correlation Engine.
Correlates alerts using:
  1. Temporal proximity (within configurable window)
  2. Label similarity (namespace, service, node, cluster)
  3. Semantic similarity on alert names (TF-IDF cosine)
  4. Causal graph analysis (parent → child alert chains)
"""

import asyncio
import hashlib
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import select, update

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.core.event_bus import event_bus
from app.models.alert_model import Alert, CorrelationGroup
from app.services.topology_service import get_topology_service, primary_entity

logger = logging.getLogger(__name__)


def _group_signature(alert_ids: List[str]) -> str:
    """Stable hash of the member set, so the same cluster maps to one group."""
    joined = ",".join(sorted(str(a) for a in alert_ids if a))
    return hashlib.sha256(joined.encode()).hexdigest()[:32]


class AlertCorrelationService:
    def __init__(self):
        self._alert_buffer: deque = deque(maxlen=settings.MAX_ALERTS_MEMORY)
        self._correlation_groups: Dict[str, dict] = {}
        self._vectorizer = TfidfVectorizer(
            analyzer="word",
            ngram_range=(1, 2),
            max_features=1000,
            lowercase=True,
        )
        self._fitted = False

    def _label_similarity(self, a: dict, b: dict) -> float:
        """Jaccard similarity between label sets."""
        labels_a = set(f"{k}={v}" for k, v in (a.get("labels") or {}).items())
        labels_b = set(f"{k}={v}" for k, v in (b.get("labels") or {}).items())
        if not labels_a and not labels_b:
            return 0.5
        if not labels_a or not labels_b:
            return 0.0
        intersection = labels_a & labels_b
        union = labels_a | labels_b
        return len(intersection) / len(union)

    def _namespace_match(self, a: dict, b: dict) -> float:
        """Bonus score if same namespace/service/node."""
        score = 0.0
        for field in ("namespace", "service", "node"):
            va = a.get(field) or a.get("labels", {}).get(field)
            vb = b.get(field) or b.get("labels", {}).get(field)
            if va and vb and va == vb:
                score += 0.15
        return min(score, 0.45)

    def _temporal_score(self, a: dict, b: dict) -> float:
        """Decaying temporal similarity within correlation window."""
        try:
            ta = datetime.fromisoformat(a.get("starts_at") or a.get("detected_at", ""))
            tb = datetime.fromisoformat(b.get("starts_at") or b.get("detected_at", ""))
        except (ValueError, TypeError):
            return 0.0
        delta = abs((ta - tb).total_seconds())
        window = settings.CORRELATION_WINDOW_SECONDS
        if delta > window:
            return 0.0
        return 1.0 - (delta / window)

    def _semantic_score(self, names_a: List[str], names_b: List[str]) -> float:
        """TF-IDF cosine similarity between alert name texts."""
        corpus = names_a + names_b
        try:
            if not self._fitted or len(corpus) < 2:
                self._vectorizer.fit(corpus)
                self._fitted = True
            vec_a = self._vectorizer.transform(names_a)
            vec_b = self._vectorizer.transform(names_b)
            sim = cosine_similarity(vec_a, vec_b)
            return float(sim.mean())
        except Exception:
            return 0.0

    def _pair_score(self, a: dict, b: dict) -> float:
        temporal = self._temporal_score(a, b)
        if temporal == 0.0:
            return 0.0  # outside window; skip
        label_sim = self._label_similarity(a, b)
        ns_bonus = self._namespace_match(a, b)
        sem = self._semantic_score(
            [a.get("name", "")],
            [b.get("name", "")],
        )
        # Weighted sum
        score = (
            0.35 * temporal +
            0.25 * label_sim +
            0.25 * ns_bonus / 0.45 +
            0.15 * sem
        )
        return round(score, 4)

    def _detect_pattern(self, alerts: List[dict]) -> str:
        """Classify correlation pattern."""
        if len(alerts) <= 1:
            return "singleton"

        services = [a.get("service") or a.get("labels", {}).get("service") for a in alerts]
        nodes = [a.get("node") or a.get("labels", {}).get("node") for a in alerts]

        unique_services = len(set(s for s in services if s))
        unique_nodes = len(set(n for n in nodes if n))

        if unique_services == 1 and len(alerts) >= 3:
            return "cascade"
        elif unique_nodes >= 3:
            return "fan-out"
        elif unique_services >= 3 and unique_nodes == 1:
            return "service-degradation"
        else:
            return "cluster"

    def correlate_alerts(self, alerts: List[dict]) -> List[dict]:
        """
        Group alerts into correlation clusters using greedy single-linkage.
        Returns list of correlation group dicts.
        """
        if len(alerts) < 2:
            return []

        n = len(alerts)
        scores = np.zeros((n, n))
        for i in range(n):
            for j in range(i + 1, n):
                s = self._pair_score(alerts[i], alerts[j])
                scores[i][j] = scores[j][i] = s

        # Union-Find clustering with threshold 0.4
        threshold = 0.4
        parent = list(range(n))

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py

        for i in range(n):
            for j in range(i + 1, n):
                if scores[i][j] >= threshold:
                    union(i, j)

        groups: Dict[int, List[int]] = defaultdict(list)
        for i in range(n):
            groups[find(i)].append(i)

        result = []
        for root_idx, members in groups.items():
            if len(members) < 2:
                continue
            group_alerts = [alerts[i] for i in members]
            group_scores = [scores[members[0]][m] for m in members[1:]]
            avg_score = float(np.mean(group_scores)) if group_scores else 0.0

            # Root cause candidate: first alert by time
            sorted_alerts = sorted(
                group_alerts,
                key=lambda a: a.get("starts_at") or a.get("detected_at") or "",
            )

            pattern = self._detect_pattern(group_alerts)

            member_ids = [a.get("id") for a in group_alerts]
            group = {
                "alert_ids": member_ids,
                "signature": _group_signature(member_ids),
                "root_alert_id": sorted_alerts[0].get("id"),
                "similarity_score": round(avg_score, 4),
                "pattern": pattern,
                "created_at": datetime.utcnow().isoformat(),
                "window_start": sorted_alerts[0].get("starts_at"),
                "window_end": sorted_alerts[-1].get("starts_at"),
                "labels": {
                    "namespaces": list({
                        a.get("namespace") for a in group_alerts if a.get("namespace")
                    }),
                    "services": list({
                        a.get("service") for a in group_alerts if a.get("service")
                    }),
                },
                "_alerts": group_alerts,  # in-memory only
            }
            result.append(group)

        return result

    async def start_correlation_loop(self):
        """Periodically correlate recent alerts."""
        logger.info("🔗 Alert correlation loop started")
        while True:
            try:
                await self._run_correlation()
            except Exception as e:
                logger.error(f"Correlation loop error: {e}")
            await asyncio.sleep(settings.CORRELATION_WINDOW_SECONDS)

    async def _run_correlation(self):
        async with AsyncSessionLocal() as db:
            window_start = datetime.utcnow() - timedelta(
                seconds=settings.CORRELATION_WINDOW_SECONDS * 2
            )
            stmt = (
                select(Alert)
                .where(Alert.created_at >= window_start)
                .order_by(Alert.created_at.asc())
                .limit(500)
            )
            result = await db.execute(stmt)
            alerts = result.scalars().all()

        if len(alerts) < 2:
            return

        alert_dicts = [
            {
                "id": a.id,
                "name": a.name,
                "severity": a.severity,
                "namespace": a.namespace,
                "service": a.service,
                "node": a.node,
                "labels": a.labels or {},
                "starts_at": a.starts_at.isoformat() if a.starts_at else None,
            }
            for a in alerts
        ]

        # Update the dependency topology from this window, then decay stale edges.
        topo = get_topology_service()
        try:
            await topo.record_alerts(alert_dicts)
            await topo.decay_and_prune()
        except Exception as e:
            logger.debug(f"Topology update skipped: {e}")

        groups = self.correlate_alerts(alert_dicts)

        new_groups: List[Tuple[str, dict, dict]] = []  # (cg_id, group, subgraph)
        async with AsyncSessionLocal() as db:
            for group in groups:
                # Dedup: skip clusters we've already recorded (same member set).
                existing = await db.execute(
                    select(CorrelationGroup).where(
                        CorrelationGroup.signature == group["signature"]
                    ).limit(1)
                )
                if existing.scalars().first():
                    continue

                # Reinforce temporal edges from this (time-ordered) group, then
                # let the topology pick the causal root.
                member_alerts = group.get("_alerts") or []
                try:
                    await topo.learn_from_group(member_alerts)
                except Exception as e:
                    logger.debug(f"Topology learn skipped: {e}")

                ent_by_alert = {a.get("id"): primary_entity(a) for a in member_alerts}
                entity_keys = [e[0] for e in ent_by_alert.values() if e]
                subgraph = {}
                root_entity = None
                root_alert_id = group["root_alert_id"]
                try:
                    subgraph = await topo.get_subgraph(entity_keys)
                    root_entity = subgraph.get("root")
                    if root_entity:
                        # Earliest alert whose primary entity is the causal root.
                        for a in member_alerts:
                            pe = ent_by_alert.get(a.get("id"))
                            if pe and pe[0] == root_entity:
                                root_alert_id = a.get("id")
                                break
                except Exception as e:
                    logger.debug(f"Topology root selection skipped: {e}")

                existing_ids = set(group.get("alert_ids") or [])
                cg = CorrelationGroup(
                    alert_ids=group["alert_ids"],
                    signature=group["signature"],
                    root_alert_id=root_alert_id,
                    similarity_score=group["similarity_score"],
                    pattern=group["pattern"],
                    window_start=datetime.fromisoformat(group["window_start"])
                    if group.get("window_start") else None,
                    window_end=datetime.fromisoformat(group["window_end"])
                    if group.get("window_end") else None,
                    labels=group.get("labels"),
                    root_entity=root_entity,
                    topology=subgraph or None,
                    inference_status="pending",
                )
                db.add(cg)
                await db.flush()

                await db.execute(
                    update(Alert)
                    .where(Alert.id.in_(list(existing_ids)))
                    .values(correlation_group_id=cg.id)
                )
                await db.commit()

                group["id"] = cg.id
                await event_bus.publish("correlations", group)
                logger.info(
                    f"🔗 Correlation group: {len(group['alert_ids'])} alerts, "
                    f"pattern={group['pattern']}, score={group['similarity_score']}, "
                    f"root={root_entity}"
                )
                new_groups.append((cg.id, group, subgraph))

        # Auto-generate the llama inference for each NEW group (once per group).
        for cg_id, group, subgraph in new_groups:
            await self._infer_group(cg_id, group.get("_alerts") or [],
                                    group.get("pattern", ""), group.get("similarity_score", 0.0),
                                    subgraph)

    async def _infer_group(
        self, cg_id: str, alerts: List[dict], pattern: str, similarity: float,
        topology: Optional[dict] = None,
    ):
        """Run the LLM inference for one group and store it. Tolerant of failure."""
        from app.services.rca_service import get_rca_service
        try:
            inference = await get_rca_service().generate_correlation_inference(
                alerts, pattern=pattern, similarity=similarity, topology=topology
            )
            status = "completed"
        except Exception as e:
            logger.warning(f"Inference generation failed for group {cg_id}: {e}")
            inference, status = None, "failed"

        async with AsyncSessionLocal() as db:
            cg = await db.get(CorrelationGroup, cg_id)
            if cg:
                cg.inference = inference
                cg.inference_status = status
                await db.commit()
        if status == "completed":
            await event_bus.publish("correlations", {"id": cg_id, "inference": inference,
                                                     "inference_status": status})
            logger.info(f"🧠 Inference ready for group {cg_id}: {inference.get('title')}")

    async def regenerate_inference(self, cg_id: str) -> dict:
        """Manual re-run: reload a group's alerts and regenerate its inference."""
        async with AsyncSessionLocal() as db:
            cg = await db.get(CorrelationGroup, cg_id)
            if not cg:
                raise ValueError(f"Correlation group {cg_id} not found")
            alert_ids = cg.alert_ids or []
            pattern, similarity = cg.pattern or "", cg.similarity_score or 0.0
            topology = cg.topology
            res = await db.execute(select(Alert).where(Alert.id.in_(alert_ids)))
            db_alerts = res.scalars().all()
        alerts = [
            {
                "id": a.id, "name": a.name, "severity": a.severity,
                "namespace": a.namespace, "service": a.service, "node": a.node,
                "labels": a.labels or {}, "annotations": a.annotations,
                "starts_at": a.starts_at.isoformat() if a.starts_at else None,
            }
            for a in db_alerts
        ]
        await self._infer_group(cg_id, alerts, pattern, similarity, topology)
        async with AsyncSessionLocal() as db:
            cg = await db.get(CorrelationGroup, cg_id)
            return {"id": cg_id, "inference": cg.inference if cg else None,
                    "inference_status": cg.inference_status if cg else "failed"}


# Singleton
_correlation_service: Optional[AlertCorrelationService] = None


def get_correlation_service() -> AlertCorrelationService:
    global _correlation_service
    if _correlation_service is None:
        _correlation_service = AlertCorrelationService()
    return _correlation_service
