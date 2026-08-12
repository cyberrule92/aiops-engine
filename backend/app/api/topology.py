"""topology.py - Service/infrastructure dependency graph endpoints."""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from app.services.topology_service import get_topology_service

router = APIRouter()


@router.get("/topology")
async def get_topology(
    view: str = Query("infra", description="infra | all"),
    limit: int = Query(300, le=2000),
):
    """Dependency graph. Default 'infra' shows only infrastructure/network CIs
    (hosts, instances, managers, clusters, subnets, interfaces) with hierarchy
    tiers — the ServiceNow-ITOM style map. 'all' includes app services."""
    view = view if view in ("infra", "all") else "infra"
    return await get_topology_service().get_graph(limit=limit, view=view)


@router.get("/topology/dependencies")
async def get_dependencies(
    entity: str = Query(..., description="entity key, e.g. host:web-01 or subnet:10.0.0.0/24"),
    depth: int = Query(2, ge=1, le=4),
):
    """CI-centered dependency view: upstream (depends-on/contains) + downstream."""
    res = await get_topology_service().get_dependencies(entity.strip(), depth=depth)
    if res.get("focus") is None:
        raise HTTPException(404, f"Entity not found: {entity}")
    return res


@router.post("/topology/rebuild")
async def rebuild_topology(limit: int = Query(2000, le=10000)):
    """(Re)build the topology graph from the most recent alerts."""
    return await get_topology_service().rebuild_from_alerts(limit=limit)


class DeclareEdge(BaseModel):
    src: str   # entity key, e.g. "host:web-01" or "subnet:10.0.0.0/24"
    dst: str


@router.post("/topology/edges")
async def declare_edge(req: DeclareEdge):
    """Declare an explicit dependency edge (src affects dst)."""
    if not req.src or not req.dst or req.src == req.dst:
        raise HTTPException(400, "src and dst must be distinct non-empty entity keys")
    return await get_topology_service().declare_edge(req.src.strip(), req.dst.strip())
