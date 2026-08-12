"""
RCA (Root Cause Analysis) Service powered by local Ollama LLM.
Constructs rich context from alert correlation groups, anomalies,
and metric history, then queries Ollama for structured RCA output.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy import select, desc

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.models.alert_model import RCAReport, CorrelationGroup, Alert, Anomaly

logger = logging.getLogger(__name__)

# Per-target locks so concurrent/repeat RCA requests for the same group/alert
# don't each kick off a (slow) LLM call. Keyed by dedup key.
_rca_locks: Dict[str, asyncio.Lock] = {}


def _rca_dedup_key(
    correlation_group_id: Optional[str],
    alert_id: Optional[str],
    anomaly_id: Optional[str],
) -> Optional[str]:
    """A stable key identifying the RCA target, or None for ad-hoc multi-target sets."""
    if correlation_group_id:
        return f"cg:{correlation_group_id}"
    if alert_id:
        return f"alert:{alert_id}"
    if anomaly_id:
        return f"anomaly:{anomaly_id}"
    return None

RCA_SYSTEM_PROMPT = """You are a principal Site Reliability Engineer running a rigorous,
evidence-based root cause analysis (RCA) of a Kubernetes incident. You reason like an
investigator: you GENERATE several competing hypotheses, weigh the specific evidence FOR and
AGAINST each, ELIMINATE the ones the evidence contradicts, and converge on the single most
probable root cause. You never give generic, copy-paste advice.

You are given an incident context: correlated alerts, metric anomalies (value vs baseline vs
deviation), a chronological timeline of what happened in what order, and a dependency topology
(src affects dst) with a suggested causal root.

HARD RULES:
- BE SPECIFIC. Every statement must reference concrete things from the context: exact alert
  names, service / namespace / node names, metric names, numeric values, deviations, and
  timestamps. Generic phrasing ("check the logs", "restart the service", "investigate the
  issue", "monitor the system") is FORBIDDEN unless you name WHICH component, WHICH log or
  metric, and WHAT to look for.
- SHOW YOUR WORK. `hypotheses` must contain 2-4 candidate root causes. For each, give a
  verdict (accepted | rejected | inconclusive) and the specific evidence_for / evidence_against
  drawn from the context. Explicitly eliminate the rejected ones.
- REASON CAUSALLY using the dependency topology (a failing upstream entity producing downstream
  symptoms), not merely by correlation/co-occurrence.
- SPLIT remediation into short_term_actions (stop the bleeding now) and long_term_actions
  (prevent recurrence). Each action is concrete; include an exact command/PromQL/kubectl in the
  `command` field where applicable.
- `confidence` (0.0-1.0) must reflect how decisively the evidence isolates ONE cause.
- A factual, complete event timeline is attached to the report automatically from the data —
  do NOT emit a timeline field; instead USE the timeline ordering in the context to reason.

Respond ONLY with valid JSON in exactly this structure:
{
  "probable_root_cause": "Definitive 1-3 sentence statement of the single most probable root cause, naming the specific failing entity.",
  "confidence": 0.0,
  "problem_overview": "2-4 sentences: what is happening and the blast radius, in plain language.",
  "impact": "Concrete user/service/SLO impact, referencing affected components.",
  "supporting_evidence": [
    {"observation": "specific fact from context (e.g. metric=value, +X% deviation, alert name)", "implication": "what it tells us"}
  ],
  "hypotheses": [
    {"hypothesis": "candidate root cause", "verdict": "accepted|rejected|inconclusive", "evidence_for": ["..."], "evidence_against": ["..."], "reasoning": "why accepted or eliminated"}
  ],
  "reasoning": "Concise chain-of-thought tying the evidence to the elimination of alternatives and the final conclusion.",
  "short_term_actions": [
    {"action": "specific immediate step", "rationale": "why this helps now", "command": "exact kubectl/promql or empty string"}
  ],
  "long_term_actions": [
    {"action": "specific preventive change", "rationale": "why it prevents recurrence"}
  ],
  "contributing_factors": ["specific factor 1", "specific factor 2"],
  "related_components": ["service-a", "node-1", "ns/x"],
  "alert_pattern": "cascade|fan-out|oscillation|spike|degradation",
  "estimated_ttf": "e.g. '15 minutes'"
}"""


CORRELATION_SYSTEM_PROMPT = """You are an AIOps correlation engine. You are given a set of
Kubernetes alerts that have already been statistically grouped as related. Your job is to
infer the SINGLE underlying incident that ties them together and identify the most likely
ROOT/CAUSAL alert (the one that probably triggered the others).

Respond ONLY in valid JSON with exactly this structure:
{
  "title": "Short incident title (<= 10 words)",
  "summary": "2-3 sentence explanation of what is happening and why these alerts are one incident",
  "severity": "critical|warning|info",
  "root_cause_alert_id": "<the exact id of the most likely causal alert from the list>",
  "causal_chain": ["alert X likely caused alert Y because ...", "..."],
  "affected_components": ["service-a", "node-1", "namespace-x"],
  "confidence": 0.0
}

root_cause_alert_id MUST be one of the provided alert ids. Reason causally (e.g. a node
failure causing pod restarts causing elevated error rates), not just by similarity."""


class RCAService:
    def __init__(self):
        self._client = httpx.AsyncClient(
            base_url=settings.OLLAMA_BASE_URL,
            timeout=settings.OLLAMA_TIMEOUT,
        )

    @staticmethod
    def _parse_ts(v) -> Optional[datetime]:
        """Best-effort parse of an ISO timestamp (str or datetime) → datetime."""
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        try:
            return datetime.fromisoformat(str(v).replace("Z", "+00:00").replace("+00:00", ""))
        except Exception:
            return None

    def _build_timeline(self, alerts: List[dict], anomalies: List[dict]) -> List[tuple]:
        """Merge alerts + anomalies into one ascending-time event list.

        Returns tuples (dt, kind, label) sorted oldest→newest so the LLM (and the
        prompt reader) can see the order events actually unfolded in.
        """
        events: List[tuple] = []
        for a in alerts:
            dt = self._parse_ts(a.get("starts_at"))
            sev = str(a.get("severity", "")).upper()
            label = (f"[{sev}] {a.get('name','alert')} "
                     f"(svc={a.get('service') or 'N/A'}, node={a.get('node') or 'N/A'}, "
                     f"ns={a.get('namespace') or 'N/A'})")
            events.append((dt, "alert", label))
        for an in anomalies:
            dt = self._parse_ts(an.get("detected_at"))
            dev = an.get("deviation_pct")
            dev_s = f"{dev:+.1f}%" if isinstance(dev, (int, float)) else "n/a"
            label = (f"ANOMALY {an.get('metric_name','metric')} "
                     f"(value={an.get('metric_value')}, baseline={an.get('baseline_value')}, "
                     f"dev={dev_s}, score={an.get('score')})")
            events.append((dt, "anomaly", label))
        # Sort: timestamped events ascending first, undated last (stable).
        dated = sorted([e for e in events if e[0] is not None], key=lambda e: e[0])
        undated = [e for e in events if e[0] is None]
        return dated + undated

    def _timeline_dicts(self, alerts: List[dict], anomalies: List[dict]) -> List[dict]:
        """Factual timeline as [{time, event, type}] — built from the data, not the
        LLM, so it is always complete and accurate (absolute time + +Ns offset)."""
        tl = self._build_timeline(alerts, anomalies)
        t0 = next((dt for dt, _, _ in tl if dt is not None), None)
        out: List[dict] = []
        for dt, kind, label in tl[:60]:
            if dt is not None and t0 is not None:
                offset = int((dt - t0).total_seconds())
                stamp = f"{dt.strftime('%H:%M:%S')} (+{offset}s)"
            elif dt is not None:
                stamp = dt.strftime("%H:%M:%S")
            else:
                stamp = ""
            out.append({"time": stamp, "event": label, "type": kind})
        return out

    def _build_context(
        self,
        alerts: List[dict],
        anomalies: List[dict],
        correlation_group: Optional[dict] = None,
    ) -> str:
        lines = [
            "=== INCIDENT CONTEXT ===",
            f"Analysis Time: {datetime.utcnow().isoformat()}",
            "",
        ]

        if correlation_group:
            lines += [
                "=== CORRELATION GROUP ===",
                f"Pattern: {correlation_group.get('pattern', 'unknown')}",
                f"Similarity Score: {correlation_group.get('similarity_score', 0)}",
                f"Affected Namespaces: {', '.join(correlation_group.get('labels', {}).get('namespaces', []))}",
                f"Affected Services: {', '.join(correlation_group.get('labels', {}).get('services', []))}",
            ]
            if correlation_group.get("root_entity"):
                lines.append(f"Topology root entity (causal-graph suggested): {correlation_group['root_entity']}")
            lines += self._topology_lines(correlation_group.get("topology"))
            lines.append("")

        # Chronological timeline — the order of events is primary RCA evidence.
        timeline = self._build_timeline(alerts, anomalies)
        if timeline:
            t0 = next((dt for dt, _, _ in timeline if dt is not None), None)
            lines.append("=== CHRONOLOGICAL TIMELINE (oldest first) ===")
            for dt, kind, label in timeline[:40]:
                if dt is not None:
                    offset = f"+{int((dt - t0).total_seconds())}s" if t0 else ""
                    stamp = f"{dt.isoformat()} ({offset})"
                else:
                    stamp = "time-unknown"
                lines.append(f"  {stamp} | {kind}: {label}")
            if t0:
                lines.append(f"(t0 = {t0.isoformat()}; the FIRST event is often nearest the root cause)")
            lines.append("")

        if alerts:
            lines.append("=== FIRING ALERTS (detail) ===")
            for a in alerts[:20]:  # cap to avoid token overflow
                severity = a.get("severity", "unknown").upper()
                name = a.get("name", "unknown")
                ns = a.get("namespace") or "N/A"
                svc = a.get("service") or "N/A"
                node = a.get("node") or "N/A"
                starts = a.get("starts_at") or "N/A"
                lines.append(
                    f"  [{severity}] {name} | ns={ns} | svc={svc} | node={node} | started={starts}"
                )
                if a.get("annotations"):
                    desc = a["annotations"].get("description") or a["annotations"].get("summary")
                    if desc:
                        lines.append(f"    Description: {desc}")
            lines.append("")

        if anomalies:
            lines.append("=== METRIC ANOMALIES (evidence) ===")
            for an in anomalies[:10]:
                metric = an.get("metric_name", "unknown")
                score = an.get("score", 0) or 0
                val = an.get("metric_value", "N/A")
                baseline = an.get("baseline_value", "N/A")
                dev = an.get("deviation_pct", 0) or 0
                svc = an.get("service") or "N/A"
                lines.append(
                    f"  {metric} | score={score:.2f} | value={val} | baseline={baseline} | "
                    f"deviation={dev:+.1f}% | service={svc}"
                )
            lines.append("")

        lines.append("=== TASK ===")
        lines.append(
            "Perform the investigative RCA per your instructions. Generate competing hypotheses, "
            "weigh the specific evidence above for and against each, eliminate the contradicted "
            "ones, and converge on the most probable root cause. Use the timeline ordering and "
            "the dependency topology to reason causally. Cite exact entities, metrics, values and "
            "timestamps — no generic advice. Return ONLY the required JSON."
        )

        return "\n".join(lines)

    async def _call_ollama(self, context: str) -> str:
        """Call Ollama API with the RCA prompt."""
        payload = {
            "model": settings.OLLAMA_MODEL,
            "messages": [
                {"role": "system", "content": RCA_SYSTEM_PROMPT},
                {"role": "user", "content": context},
            ],
            "stream": False,
            "format": "json",
            "options": {
                "temperature": 0.15,
                "top_p": 0.9,
                # Richer structured output (timeline + hypotheses + evidence + actions)
                # needs more headroom than the old 1-cause schema.
                "num_predict": 2048,
            },
        }

        try:
            resp = await self._client.post("/api/chat", json=payload)
            resp.raise_for_status()
            data = resp.json()
            content = data.get("message", {}).get("content", "{}")
            return content
        except httpx.ConnectError:
            logger.warning("Ollama not reachable — returning fallback RCA")
            return json.dumps({
                "probable_root_cause": "Ollama LLM not reachable — automated RCA could not run. "
                                       f"Verify connectivity to {settings.OLLAMA_BASE_URL}.",
                "confidence": 0.0,
                "problem_overview": "The RCA engine could not reach the local Ollama model, so no "
                                    "analysis was generated for this incident.",
                "impact": "RCA analysis unavailable.",
                "timeline": [],
                "supporting_evidence": [],
                "hypotheses": [],
                "reasoning": "",
                "short_term_actions": [
                    {"action": "Verify the Ollama pod is running", "rationale": "RCA depends on it",
                     "command": "kubectl get pods -l app=ollama -n observability"},
                    {"action": "Check the Ollama service endpoint", "rationale": "Confirm reachability",
                     "command": "kubectl get svc -n observability | grep ollama"},
                    {"action": "Ensure the model is pulled", "rationale": "Inference needs the model",
                     "command": f"kubectl exec deploy/ollama -n observability -- ollama pull {settings.OLLAMA_MODEL}"},
                ],
                "long_term_actions": [],
                "contributing_factors": ["Ollama service unreachable at " + settings.OLLAMA_BASE_URL],
                "related_components": [],
                "alert_pattern": "unknown",
                "estimated_ttf": "N/A",
            })
        except Exception as e:
            logger.error(f"Ollama RCA call failed: {e}")
            raise

    @staticmethod
    def _normalize_rca(data: dict) -> dict:
        """Coerce raw LLM JSON into the canonical RCA shape, tolerating the older
        single-cause schema and filling legacy aliases for backward compatibility."""
        def as_list(v):
            return v if isinstance(v, list) else ([v] if v else [])

        # probable_root_cause is the new primary; fall back to legacy root_cause.
        probable = (data.get("probable_root_cause") or data.get("root_cause") or "").strip()
        short_term = as_list(data.get("short_term_actions"))
        long_term = as_list(data.get("long_term_actions"))

        # Legacy flat remediation_steps: prefer explicit field, else synthesize from
        # the new short+long action objects so old UIs/clients keep working.
        legacy_steps = as_list(data.get("remediation_steps"))
        if not legacy_steps:
            def _txt(a):
                if isinstance(a, dict):
                    s = a.get("action", "")
                    return f"{s} — {a['command']}" if a.get("command") else s
                return str(a)
            legacy_steps = [_txt(a) for a in (short_term + long_term)]

        try:
            confidence = float(data.get("confidence", 0.5) or 0.5)
        except (TypeError, ValueError):
            confidence = 0.5

        return {
            "probable_root_cause": probable,
            "root_cause": probable,  # legacy alias
            "confidence": max(0.0, min(1.0, confidence)),
            "problem_overview": data.get("problem_overview") or "",
            "impact": data.get("impact") or "",
            "timeline": as_list(data.get("timeline")),
            "supporting_evidence": as_list(data.get("supporting_evidence")),
            "hypotheses": as_list(data.get("hypotheses")),
            "reasoning": data.get("reasoning") or "",
            "short_term_actions": short_term,
            "long_term_actions": long_term,
            "remediation_steps": legacy_steps,  # legacy alias
            "contributing_factors": as_list(data.get("contributing_factors")),
            "related_components": as_list(data.get("related_components")),
            "alert_pattern": data.get("alert_pattern") or "unknown",
            "estimated_ttf": data.get("estimated_ttf") or "N/A",
        }

    @staticmethod
    def _report_to_result(r: RCAReport, reused: bool) -> dict:
        return {
            "id": r.id,
            "probable_root_cause": r.root_cause,
            "root_cause": r.root_cause,
            "confidence": r.confidence,
            "problem_overview": r.problem_overview or "",
            "impact": r.impact or "",
            "timeline": r.timeline or [],
            "supporting_evidence": r.supporting_evidence or [],
            "hypotheses": r.hypotheses or [],
            "reasoning": r.reasoning or "",
            "short_term_actions": r.short_term_actions or [],
            "long_term_actions": r.long_term_actions or [],
            "contributing_factors": r.contributing_factors or [],
            "remediation_steps": r.remediation_steps or [],
            "related_components": r.related_components or [],
            "correlation_group_id": r.correlation_group_id,
            "alert_id": r.alert_id,
            "anomaly_id": r.anomaly_id,
            "llm_model": r.llm_model,
            "status": r.status,
            "reused": reused,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }

    async def _find_existing_rca(
        self,
        correlation_group_id: Optional[str],
        alert_id: Optional[str],
        anomaly_id: Optional[str],
    ) -> Optional[RCAReport]:
        """Return a reusable RCA for this target, if any.

        Priority: a completed report always wins. Otherwise a *fresh* in-progress
        report (so concurrent requests dedup). Stale in-progress reports — left
        behind by a crash or a disconnected client — are marked failed and ignored
        so they can never permanently block regeneration. 'failed' is never reused.
        """
        if correlation_group_id:
            col, val = RCAReport.correlation_group_id, correlation_group_id
        elif alert_id:
            col, val = RCAReport.alert_id, alert_id
        elif anomaly_id:
            col, val = RCAReport.anomaly_id, anomaly_id
        else:
            return None

        # An in-progress report older than this is considered abandoned.
        stale_cutoff = datetime.utcnow() - timedelta(seconds=max(settings.OLLAMA_TIMEOUT * 2, 300))

        async with AsyncSessionLocal() as db:
            completed = (await db.execute(
                select(RCAReport)
                .where(col == val, RCAReport.status == "completed")
                .order_by(desc(RCAReport.created_at))
                .limit(1)
            )).scalars().first()
            if completed is not None:
                return completed

            in_progress = (await db.execute(
                select(RCAReport)
                .where(col == val, RCAReport.status == "in_progress")
                .order_by(desc(RCAReport.created_at))
            )).scalars().all()

            fresh = None
            changed = False
            for rep in in_progress:
                if rep.created_at and rep.created_at >= stale_cutoff and fresh is None:
                    fresh = rep
                elif rep.status == "in_progress":
                    rep.status = "failed"  # reap the abandoned placeholder
                    changed = True
            if changed:
                await db.commit()
            return fresh

    async def generate_rca(
        self,
        alerts: List[dict],
        anomalies: List[dict],
        correlation_group: Optional[dict] = None,
        correlation_group_id: Optional[str] = None,
        alert_id: Optional[str] = None,
        anomaly_id: Optional[str] = None,
    ) -> dict:
        """Generate RCA and persist to DB, skipping work if one already exists.

        If an RCA for the same target is completed or in progress, that report is
        returned (with reused=True) instead of starting another LLM call.
        """
        key = _rca_dedup_key(correlation_group_id, alert_id, anomaly_id)
        report_id: Optional[str] = None

        if key is not None:
            lock = _rca_locks.setdefault(key, asyncio.Lock())
            async with lock:
                existing = await self._find_existing_rca(
                    correlation_group_id, alert_id, anomaly_id
                )
                if existing is not None:
                    logger.info(f"♻️  Reusing RCA {existing.id} for {key} (status={existing.status})")
                    return self._report_to_result(existing, reused=True)
                # Reserve the slot so concurrent callers see it as in-progress.
                async with AsyncSessionLocal() as db:
                    placeholder = RCAReport(
                        correlation_group_id=correlation_group_id,
                        alert_id=alert_id,
                        anomaly_id=anomaly_id,
                        root_cause="Analysis in progress…",
                        contributing_factors=[],
                        remediation_steps=[],
                        confidence=0.0,
                        llm_model=settings.OLLAMA_MODEL,
                        status="in_progress",
                    )
                    db.add(placeholder)
                    await db.commit()
                    await db.refresh(placeholder)
                    report_id = placeholder.id

        context = self._build_context(alerts, anomalies, correlation_group)
        try:
            raw_response = await self._call_ollama(context)
        except BaseException:
            # Includes asyncio.CancelledError (client disconnect): release the
            # reserved slot so it doesn't get stuck in_progress.
            if report_id is not None:
                async with AsyncSessionLocal() as db:
                    rep = await db.get(RCAReport, report_id)
                    if rep:
                        rep.status = "failed"
                        await db.commit()
            raise

        # Parse JSON from LLM (tolerate prose wrapping the JSON block).
        raw_data: Dict[str, Any]
        try:
            raw_data = json.loads(raw_response)
        except json.JSONDecodeError:
            import re
            match = re.search(r"\{.*\}", raw_response, re.DOTALL)
            try:
                raw_data = json.loads(match.group()) if match else {}
            except Exception:
                raw_data = {}
            if not raw_data:
                # Couldn't parse structured output — degrade gracefully.
                raw_data = {
                    "probable_root_cause": raw_response[:500],
                    "confidence": 0.3,
                    "problem_overview": "LLM returned unstructured output; see raw response.",
                }

        rca = self._normalize_rca(raw_data)

        # Replace the LLM's (often lossy) timeline with the factual one built from
        # the actual alert/anomaly timestamps — complete, ordered, and accurate.
        det_timeline = self._timeline_dicts(alerts, anomalies)
        if det_timeline:
            rca["timeline"] = det_timeline

        # Persist: fill the reserved placeholder if we created one, else insert.
        async with AsyncSessionLocal() as db:
            report = await db.get(RCAReport, report_id) if report_id else None
            if report is None:
                report = RCAReport(
                    correlation_group_id=correlation_group_id,
                    alert_id=alert_id,
                    anomaly_id=anomaly_id,
                )
                db.add(report)
            report.root_cause = rca["probable_root_cause"]
            report.confidence = rca["confidence"]
            report.problem_overview = rca["problem_overview"]
            report.impact = rca["impact"]
            report.timeline = rca["timeline"]
            report.supporting_evidence = rca["supporting_evidence"]
            report.hypotheses = rca["hypotheses"]
            report.reasoning = rca["reasoning"]
            report.short_term_actions = rca["short_term_actions"]
            report.long_term_actions = rca["long_term_actions"]
            report.remediation_steps = rca["remediation_steps"]
            report.contributing_factors = rca["contributing_factors"]
            report.related_components = rca["related_components"]
            report.llm_model = settings.OLLAMA_MODEL
            report.raw_llm_response = raw_response
            report.status = "completed"
            await db.commit()
            await db.refresh(report)

        return {
            "id": report.id,
            **rca,
            "status": "completed",
            "reused": False,
            "created_at": report.created_at.isoformat(),
            "llm_model": settings.OLLAMA_MODEL,
        }

    async def generate_rca_for_group(self, group_id: str) -> dict:
        """Load a correlation group and its alerts, then generate RCA."""
        async with AsyncSessionLocal() as db:
            from sqlalchemy import select
            cg = await db.get(CorrelationGroup, group_id)
            if not cg:
                raise ValueError(f"Correlation group {group_id} not found")

            alert_ids = cg.alert_ids or []
            alerts_result = await db.execute(
                select(Alert).where(Alert.id.in_(alert_ids))
            )
            db_alerts = alerts_result.scalars().all()

            anomaly_ids = cg.anomaly_ids or []
            anomalies_result = await db.execute(
                select(Anomaly).where(Anomaly.id.in_(anomaly_ids))
            )
            db_anomalies = anomalies_result.scalars().all()

        alerts = [
            {
                "id": a.id, "name": a.name, "severity": a.severity,
                "namespace": a.namespace, "service": a.service, "node": a.node,
                "labels": a.labels, "annotations": a.annotations,
                "starts_at": a.starts_at.isoformat() if a.starts_at else None,
            }
            for a in db_alerts
        ]
        anomalies = [
            {
                "id": a.id, "metric_name": a.metric_name, "score": a.score,
                "metric_value": a.metric_value, "baseline_value": a.baseline_value,
                "deviation_pct": a.deviation_pct, "service": a.service,
                "namespace": a.namespace,
                "detected_at": a.detected_at.isoformat() if a.detected_at else None,
            }
            for a in db_anomalies
        ]

        cg_dict = {
            "pattern": cg.pattern,
            "similarity_score": cg.similarity_score,
            "labels": cg.labels or {},
            "topology": cg.topology,
            "root_entity": cg.root_entity,
        }

        return await self.generate_rca(
            alerts=alerts,
            anomalies=anomalies,
            correlation_group=cg_dict,
            correlation_group_id=group_id,
        )

    @staticmethod
    def _topology_lines(topology: Optional[dict]) -> List[str]:
        """Render the dependency subgraph as prompt context, if available."""
        if not topology or not topology.get("edges"):
            return []
        lines = ["", "=== DEPENDENCY TOPOLOGY (src affects dst) ==="]
        if topology.get("root"):
            lines.append(f"Topology-suggested root entity: {topology['root']}")
        for e in topology.get("edges", [])[:30]:
            lines.append(
                f"  {e.get('src')} → {e.get('dst')} "
                f"[{e.get('edge_type')}, w={e.get('weight')}, conf={e.get('confidence')}]"
            )
        return lines

    async def generate_correlation_inference(
        self, alerts: List[dict], pattern: str = "", similarity: float = 0.0,
        topology: Optional[dict] = None,
    ) -> dict:
        """Synthesize an 'inference alert' for a set of statistically-correlated alerts.

        The clustering (temporal + label + semantic) has already decided these
        belong together; llama's job is the causal reasoning: what single incident
        explains them and which alert is the likely root. Returns a dict with
        title/summary/severity/root_cause_alert_id/causal_chain/affected_components/confidence.
        """
        lines = [
            "These Kubernetes alerts were statistically correlated (temporal proximity, "
            "shared labels, and semantic similarity) and likely stem from ONE underlying incident.",
            f"Detected correlation pattern: {pattern or 'unknown'} (similarity {similarity}).",
            "",
            "ALERTS (id | severity | name | namespace | service | node | started):",
        ]
        for a in alerts[:25]:
            lines.append(
                f"  {a.get('id')} | {str(a.get('severity','')).upper()} | {a.get('name')} | "
                f"ns={a.get('namespace') or 'N/A'} | svc={a.get('service') or 'N/A'} | "
                f"node={a.get('node') or 'N/A'} | {a.get('starts_at') or 'N/A'}"
            )
            ann = a.get("annotations") or {}
            desc = ann.get("description") or ann.get("summary")
            if desc:
                lines.append(f"      desc: {desc}")
        lines += self._topology_lines(topology)
        if topology and topology.get("root"):
            lines.append(
                "\nThe dependency topology suggests the root entity above; prefer the alert "
                "on that entity as root_cause_alert_id unless the evidence clearly disagrees."
            )
        context = "\n".join(lines)

        payload = {
            "model": settings.OLLAMA_MODEL,
            "messages": [
                {"role": "system", "content": CORRELATION_SYSTEM_PROMPT},
                {"role": "user", "content": context},
            ],
            "stream": False,
            "format": "json",
            "options": {"temperature": 0.1, "top_p": 0.9, "num_predict": 700},
        }

        severities = [str(a.get("severity", "")).lower() for a in alerts]
        fallback_sev = (
            "critical" if "critical" in severities
            else "warning" if "warning" in severities
            else "info"
        )
        valid_ids = {a.get("id") for a in alerts}

        try:
            resp = await self._client.post("/api/chat", json=payload)
            resp.raise_for_status()
            content = resp.json().get("message", {}).get("content", "{}")
            data = json.loads(content)
        except Exception as e:
            logger.warning(f"Correlation inference failed, using heuristic fallback: {e}")
            data = {}

        # Validate / coerce, with safe fallbacks so the UI always has something.
        rc_alert = data.get("root_cause_alert_id")
        if rc_alert not in valid_ids:
            rc_alert = alerts[0].get("id") if alerts else None
        severity = str(data.get("severity", "")).lower()
        if severity not in ("critical", "warning", "info"):
            severity = fallback_sev
        return {
            "title": data.get("title") or f"Correlated incident: {pattern or 'cluster'}",
            "summary": data.get("summary") or "Multiple related alerts firing together.",
            "severity": severity,
            "root_cause_alert_id": rc_alert,
            "causal_chain": data.get("causal_chain") or [],
            "affected_components": data.get("affected_components") or [],
            "confidence": float(data.get("confidence", 0.5) or 0.5),
            "llm_model": settings.OLLAMA_MODEL,
            "generated_at": datetime.utcnow().isoformat(),
        }


_rca_service: Optional[RCAService] = None


def get_rca_service() -> RCAService:
    global _rca_service
    if _rca_service is None:
        _rca_service = RCAService()
    return _rca_service
