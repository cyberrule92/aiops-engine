"""
Service dependency topology.

Builds a directed dependency graph of infrastructure/service entities and uses it
to ground alert correlation and RCA in *causal* relationships (Smartscape-style),
rather than purely statistical similarity. Edges come from three sources:

  - structural : derived from entity containment in alert labels
                 (a node hosts services/instances → node failure propagates to them).
  - temporal   : learned from correlation groups — when entity A's alert reliably
                 precedes entity B's, A→B is reinforced (A likely affects B).
  - declared   : explicit dependencies posted via the API.

Edge weights decay over time and are pruned, so the graph tracks the live system
rather than accumulating stale links.
"""
import ipaddress
import logging
import math
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from sqlalchemy import select, delete

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.models.alert_model import TopologyNode, TopologyEdge, Alert

logger = logging.getLogger(__name__)

# Entity-type priority for root-cause tie-breaking: infrastructure tends to be
# upstream of (cause) workload issues.
_TYPE_PRIORITY = {"node": 4, "instance": 3, "service": 2, "job": 2, "namespace": 1, "pod": 2}

# The infrastructure/network view (ServiceNow-ITOM style) only shows these CI
# types — hosts/nodes and the network/grouping CIs around them, never app services.
INFRA_TYPES = ("cluster", "subnet", "manager", "host", "node", "instance", "interface", "switch", "router")

# Hierarchy tier for the dependency map (low = upstream/containing, high = leaf).
_TIER = {
    "cluster": 0, "subnet": 1, "manager": 1, "switch": 1, "router": 1,
    "host": 2, "node": 2, "instance": 3, "interface": 3,
}

# Human-readable relationship per structural edge, by (src_type, dst_type).
_RELATION = {
    ("cluster", "host"): "contains",
    ("subnet", "host"): "connects",
    ("manager", "host"): "manages",
    ("host", "instance"): "hosts",
    ("host", "interface"): "hosts",
}

_IPV4 = re.compile(r"\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\b")


def _norm(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _first_ipv4(*candidates) -> Optional[str]:
    for c in candidates:
        if not c:
            continue
        m = _IPV4.search(str(c))
        if m:
            try:
                ipaddress.ip_address(m.group(1))
                return m.group(1)
            except ValueError:
                continue
    return None


def _subnet_of(ip: Optional[str]) -> Optional[str]:
    """Coarse /24 network for an IPv4 address (network segment grouping)."""
    if not ip:
        return None
    try:
        net = ipaddress.ip_network(f"{ip}/24", strict=False)
        return str(net)
    except ValueError:
        return None


def infra_entities(alert: dict) -> Dict[str, Tuple[str, str, str]]:
    """Extract infrastructure/network CIs from an alert.

    Returns role -> (entity_key, display_name, entity_type). Roles: host,
    instance, manager, cluster, subnet, interface. Built only from fields/labels
    that actually identify infra — app services/namespaces are intentionally
    excluded from this view.
    """
    labels = alert.get("labels") or {}
    out: Dict[str, Tuple[str, str, str]] = {}

    instance = _norm(labels.get("instance"))
    host_name = _norm(
        labels.get("host") or labels.get("nodename") or alert.get("node")
        or labels.get("node")
    )
    if not host_name and instance:
        host_name = instance.split(":", 1)[0]  # strip :port → bare host/IP

    if host_name:
        out["host"] = (f"host:{host_name}", host_name, "host")
    if instance:
        out["instance"] = (f"instance:{instance}", instance, "instance")

    manager = _norm(labels.get("manager"))
    if manager:
        out["manager"] = (f"manager:{manager}", manager, "manager")

    cluster = _norm(labels.get("cluster") or labels.get("exported_cluster"))
    if cluster:
        out["cluster"] = (f"cluster:{cluster}", cluster, "cluster")

    iface = _norm(labels.get("ifName") or labels.get("ifDescr")
                  or labels.get("interface") or labels.get("ifAlias"))
    if iface:
        out["interface"] = (f"interface:{iface}", iface, "interface")

    subnet = _subnet_of(_first_ipv4(host_name, instance, labels.get("nodename"), alert.get("node")))
    if subnet:
        out["subnet"] = (f"subnet:{subnet}", subnet, "subnet")

    return out


def entities_of(alert: dict) -> Dict[str, Tuple[str, str]]:
    """Extract entities from an alert as role -> (entity_key, display_name).

    Pulls from explicit fields first, then labels. Missing roles are omitted.
    """
    labels = alert.get("labels") or {}
    roles: Dict[str, Optional[str]] = {
        "service": _norm(alert.get("service") or labels.get("service") or labels.get("job")),
        "node": _norm(alert.get("node") or labels.get("node") or labels.get("nodename")
                      or labels.get("instance")),
        "instance": _norm(labels.get("instance")),
        "namespace": _norm(alert.get("namespace") or labels.get("namespace")),
    }
    out: Dict[str, Tuple[str, str]] = {}
    for role, name in roles.items():
        if name:
            out[role] = (f"{role}:{name}", name)
    return out


def primary_entity(alert: dict) -> Optional[Tuple[str, str, str]]:
    """The entity an alert is primarily about: (entity_key, entity_type, name)."""
    ents = entities_of(alert)
    for role in ("service", "node", "instance", "namespace"):
        if role in ents:
            key, name = ents[role]
            return key, role, name
    return None


class TopologyService:
    # ---- writes ---------------------------------------------------------

    @staticmethod
    async def _upsert_node(db, key, etype, name, labels, ts, inc_alert=False):
        node = await db.get(TopologyNode, key)
        if node is None:
            node = TopologyNode(
                entity_key=key, entity_type=etype, name=name,
                labels=labels or None, alert_count=1 if inc_alert else 0,
                first_seen=ts, last_seen=ts,
            )
            db.add(node)
        else:
            node.last_seen = ts
            if inc_alert:
                node.alert_count = (node.alert_count or 0) + 1
            if labels:
                node.labels = labels

    @staticmethod
    async def _upsert_edge(db, src, dst, etype, ts, strengthen=1.0, max_weight=50.0, relation=None):
        if not src or not dst or src == dst:
            return
        existing = (await db.execute(
            select(TopologyEdge).where(
                TopologyEdge.src_key == src,
                TopologyEdge.dst_key == dst,
                TopologyEdge.edge_type == etype,
            ).limit(1)
        )).scalars().first()
        if existing is None:
            db.add(TopologyEdge(
                src_key=src, dst_key=dst, edge_type=etype, relation=relation,
                weight=min(strengthen, max_weight),
                confidence=(1.0 if etype != "temporal" else 1 - math.exp(-1 / 3.0)),
                occurrences=1, first_seen=ts, last_seen=ts,
            ))
        else:
            existing.occurrences += 1
            existing.weight = min((existing.weight or 0.0) + strengthen, max_weight)
            existing.last_seen = ts
            if relation and not existing.relation:
                existing.relation = relation
            if etype == "temporal":
                existing.confidence = 1 - math.exp(-existing.occurrences / 3.0)
            else:
                existing.confidence = 1.0

    async def _record_infra(self, db, alert, ts):
        """Add infrastructure/network CIs + typed relationship edges for one alert."""
        ents = infra_entities(alert)
        for role, (key, name, etype) in ents.items():
            await self._upsert_node(db, key, etype, name, None, ts, inc_alert=(role == "host"))
        host = ents.get("host", (None,))[0]
        # Containment / network / management edges around the host.
        for parent_role, rel in (("cluster", "contains"), ("subnet", "connects"),
                                 ("manager", "manages")):
            parent = ents.get(parent_role, (None,))[0]
            if parent and host:
                ptype = parent_role
                await self._upsert_edge(db, parent, host, "structural", ts,
                                        strengthen=0.5, relation=rel)
        # Host → its network endpoints / interfaces.
        for child_role in ("instance", "interface"):
            child = ents.get(child_role, (None,))[0]
            if host and child:
                await self._upsert_edge(db, host, child, "structural", ts,
                                        strengthen=0.5, relation="hosts")

    async def record_alerts(self, alerts: List[dict]):
        """Upsert nodes + structural edges for a batch of alerts."""
        if not settings.TOPOLOGY_ENABLED or not alerts:
            return
        ts = datetime.utcnow()
        async with AsyncSessionLocal() as db:
            # Cap graph size: skip new nodes once over the safety limit.
            count = (await db.execute(select(TopologyNode.entity_key))).scalars().all()
            node_count = len(count)
            for alert in alerts:
                ents = entities_of(alert)
                prim = primary_entity(alert)
                for role, (key, name) in ents.items():
                    if node_count >= settings.TOPOLOGY_MAX_NODES:
                        existing = await db.get(TopologyNode, key)
                        if existing is None:
                            continue
                    is_primary = bool(prim and prim[0] == key)
                    await self._upsert_node(db, key, role, name, None, ts, inc_alert=is_primary)
                    node_count += 1
                # Structural edges: node → service / node → instance (infra → workload).
                node_key = ents.get("node", (None,))[0]
                for child_role in ("service", "instance"):
                    child = ents.get(child_role, (None,))[0]
                    if node_key and child:
                        rel = "hosts" if child_role == "instance" else "runs"
                        await self._upsert_edge(db, node_key, child, "structural", ts,
                                                strengthen=0.5, relation=rel)
                # Infrastructure/network CIs + relationships (ITOM-style view).
                await self._record_infra(db, alert, ts)
            await db.commit()

    async def learn_from_group(self, ordered_alerts: List[dict]):
        """Reinforce temporal causal edges from a time-ordered correlation group.

        For ordered alert pairs on *distinct* entities, the earlier entity is
        treated as a (weak) cause of the later one.
        """
        if not settings.TOPOLOGY_ENABLED or len(ordered_alerts) < 2:
            return
        ts = datetime.utcnow()
        keys = []
        for a in ordered_alerts:
            prim = primary_entity(a)
            keys.append(prim[0] if prim else None)
        async with AsyncSessionLocal() as db:
            seen_pairs = set()
            for i in range(len(keys)):
                for j in range(i + 1, len(keys)):
                    src, dst = keys[i], keys[j]
                    if not src or not dst or src == dst or (src, dst) in seen_pairs:
                        continue
                    seen_pairs.add((src, dst))
                    await self._upsert_edge(db, src, dst, "temporal", ts, strengthen=1.0)
            await db.commit()

    async def declare_edge(self, src: str, dst: str) -> dict:
        ts = datetime.utcnow()
        async with AsyncSessionLocal() as db:
            # Ensure endpoints exist as nodes (type inferred from key prefix).
            for k in (src, dst):
                etype = k.split(":", 1)[0] if ":" in k else "service"
                name = k.split(":", 1)[1] if ":" in k else k
                await self._upsert_node(db, k, etype, name, None, ts)
            await self._upsert_edge(db, src, dst, "declared", ts, strengthen=5.0)
            await db.commit()
        return {"src": src, "dst": dst, "edge_type": "declared"}

    async def decay_and_prune(self):
        """Decay temporal/structural edge weights and drop weak/stale edges."""
        if not settings.TOPOLOGY_ENABLED:
            return
        now = datetime.utcnow()
        ttl_cutoff = now - timedelta(seconds=settings.TOPOLOGY_EDGE_TTL_SECONDS)
        async with AsyncSessionLocal() as db:
            edges = (await db.execute(select(TopologyEdge))).scalars().all()
            removed = 0
            for e in edges:
                if e.edge_type == "declared":
                    continue  # declared edges are sticky
                e.weight = (e.weight or 0.0) * settings.TOPOLOGY_EDGE_DECAY
                if e.weight < settings.TOPOLOGY_MIN_EDGE_WEIGHT or (e.last_seen and e.last_seen < ttl_cutoff):
                    await db.delete(e)
                    removed += 1
            await db.commit()
            if removed:
                logger.debug(f"🕸️  Topology pruned {removed} weak/stale edge(s)")

    # ---- reads ----------------------------------------------------------

    async def _edges_among(self, db, keys: List[str]) -> List[TopologyEdge]:
        if not keys:
            return []
        rows = (await db.execute(
            select(TopologyEdge).where(
                TopologyEdge.src_key.in_(keys), TopologyEdge.dst_key.in_(keys)
            )
        )).scalars().all()
        return rows

    async def rank_root_causes(self, entity_keys: List[str]) -> List[Tuple[str, float]]:
        """Rank entities by how 'upstream' they are within the group.

        Net causal influence = (weighted edges this entity points OUT to other
        members) − (weighted edges pointing IN from members). Highest = likely
        root. Cycle-safe (it's a net sum). Ties broken by infra-priority then
        alert volume.
        """
        keys = [k for k in dict.fromkeys(entity_keys) if k]  # dedup, drop None
        if not keys:
            return []
        async with AsyncSessionLocal() as db:
            edges = await self._edges_among(db, keys)
            nodes = {n.entity_key: n for n in (await db.execute(
                select(TopologyNode).where(TopologyNode.entity_key.in_(keys))
            )).scalars().all()}
        out_score = {k: 0.0 for k in keys}
        in_score = {k: 0.0 for k in keys}
        for e in edges:
            w = (e.weight or 0.0) * (e.confidence or 0.0)
            out_score[e.src_key] = out_score.get(e.src_key, 0.0) + w
            in_score[e.dst_key] = in_score.get(e.dst_key, 0.0) + w

        def sort_key(k):
            net = out_score.get(k, 0.0) - in_score.get(k, 0.0)
            etype = k.split(":", 1)[0]
            prio = _TYPE_PRIORITY.get(etype, 0)
            alerts = nodes[k].alert_count if k in nodes else 0
            return (net, prio, alerts)

        ranked = sorted(keys, key=sort_key, reverse=True)
        return [(k, round(out_score.get(k, 0.0) - in_score.get(k, 0.0), 4)) for k in ranked]

    async def get_subgraph(self, entity_keys: List[str]) -> dict:
        keys = [k for k in dict.fromkeys(entity_keys) if k]
        if not keys:
            return {"nodes": [], "edges": [], "root": None}
        async with AsyncSessionLocal() as db:
            nodes = (await db.execute(
                select(TopologyNode).where(TopologyNode.entity_key.in_(keys))
            )).scalars().all()
            edges = await self._edges_among(db, keys)
        ranked = await self.rank_root_causes(keys)
        return {
            "root": ranked[0][0] if ranked else None,
            "ranking": ranked,
            "nodes": [self._node_dict(n) for n in nodes],
            "edges": [self._edge_dict(e) for e in edges],
        }

    async def get_graph(self, limit: int = 200, view: str = "infra") -> dict:
        """Topology graph. view='infra' (default) returns only infrastructure /
        network CIs (hosts, instances, managers, clusters, subnets, interfaces)
        with a hierarchy tier per node — the ServiceNow-ITOM style dependency map.
        view='all' returns every entity (incl. app services).
        """
        async with AsyncSessionLocal() as db:
            stmt = select(TopologyNode)
            if view == "infra":
                stmt = stmt.where(TopologyNode.entity_type.in_(INFRA_TYPES))
            stmt = stmt.order_by(TopologyNode.alert_count.desc()).limit(limit)
            nodes = (await db.execute(stmt)).scalars().all()
            keys = [n.entity_key for n in nodes]
            edges = await self._edges_among(db, keys) if keys else []
        return {
            "view": view,
            "nodes": [self._node_dict(n) for n in nodes],
            "edges": [self._edge_dict(e) for e in edges],
        }

    async def get_dependencies(self, entity_key: str, depth: int = 2) -> dict:
        """ServiceNow-style dependency view centered on one CI: its upstream
        (what it depends on / what contains it) and downstream (what it affects),
        walked up to `depth` hops."""
        async with AsyncSessionLocal() as db:
            focus = await db.get(TopologyNode, entity_key)
            if focus is None:
                return {"focus": None, "upstream": [], "downstream": [], "nodes": [], "edges": []}

            visited = {entity_key}
            collected_edges: Dict[Tuple[str, str, str], TopologyEdge] = {}

            async def expand(keys, direction, hops):
                if hops <= 0 or not keys:
                    return
                if direction == "down":
                    rows = (await db.execute(select(TopologyEdge).where(
                        TopologyEdge.src_key.in_(keys)))).scalars().all()
                    nxt = [e.dst_key for e in rows]
                else:
                    rows = (await db.execute(select(TopologyEdge).where(
                        TopologyEdge.dst_key.in_(keys)))).scalars().all()
                    nxt = [e.src_key for e in rows]
                new_keys = []
                for e in rows:
                    collected_edges[(e.src_key, e.dst_key, e.edge_type)] = e
                for k in nxt:
                    if k not in visited:
                        visited.add(k)
                        new_keys.append(k)
                await expand(new_keys, direction, hops - 1)

            await expand([entity_key], "down", depth)
            await expand([entity_key], "up", depth)

            nodes = (await db.execute(
                select(TopologyNode).where(TopologyNode.entity_key.in_(list(visited)))
            )).scalars().all()
        edges = list(collected_edges.values())
        upstream = sorted({e.src_key for e in edges if e.dst_key == entity_key})
        downstream = sorted({e.dst_key for e in edges if e.src_key == entity_key})
        return {
            "focus": self._node_dict(focus),
            "upstream": upstream,
            "downstream": downstream,
            "nodes": [self._node_dict(n) for n in nodes],
            "edges": [self._edge_dict(e) for e in edges],
        }

    async def rebuild_from_alerts(self, limit: int = 2000) -> dict:
        """(Re)build the topology from the most recent alerts. Useful after
        adding new extraction logic or to warm a cold graph."""
        async with AsyncSessionLocal() as db:
            rows = (await db.execute(
                select(Alert).order_by(Alert.created_at.desc()).limit(limit)
            )).scalars().all()
        alerts = [
            {"name": a.name, "namespace": a.namespace, "service": a.service,
             "node": a.node, "labels": a.labels or {}}
            for a in rows
        ]
        await self.record_alerts(alerts)
        graph = await self.get_graph(view="infra")
        return {"alerts_processed": len(alerts),
                "infra_nodes": len(graph["nodes"]), "infra_edges": len(graph["edges"])}

    @staticmethod
    def _node_dict(n: TopologyNode) -> dict:
        return {
            "entity_key": n.entity_key, "entity_type": n.entity_type, "name": n.name,
            "alert_count": n.alert_count,
            "tier": _TIER.get(n.entity_type, 2),
            "last_seen": n.last_seen.isoformat() if n.last_seen else None,
        }

    @staticmethod
    def _edge_dict(e: TopologyEdge) -> dict:
        return {
            "src": e.src_key, "dst": e.dst_key, "edge_type": e.edge_type,
            "relation": e.relation,
            "weight": round(e.weight or 0.0, 3), "confidence": round(e.confidence or 0.0, 3),
            "occurrences": e.occurrences,
        }


_topology_service: Optional[TopologyService] = None


def get_topology_service() -> TopologyService:
    global _topology_service
    if _topology_service is None:
        _topology_service = TopologyService()
    return _topology_service
