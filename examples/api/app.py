import asyncio
from collections.abc import Sequence
from contextlib import asynccontextmanager

import uvicorn
from fastapi import APIRouter
from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from aiocluster import Cluster
from aiocluster import Config
from aiocluster import NodeId
from aiocluster.state import NodeState


class ClusterSnapshot(BaseModel):
    cluster_id: str
    node_states: Sequence[NodeState]
    live_nodes: Sequence[NodeId]
    dead_nodes: Sequence[NodeId]


class KeyValue(BaseModel):
    key: str
    value: str


class Key(BaseModel):
    key: str


class Status(BaseModel):
    status: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    config: Config = app.state.config
    kvs: dict[str, str] = app.state.initial_kv
    cluster = Cluster(config, initial_key_values=kvs)
    app.state.cluster = cluster
    async with cluster:
        yield
        await cluster.shutdown()


router = APIRouter(prefix="", tags=["cluster"])


@router.get("/", include_in_schema=False)
def index():
    return RedirectResponse(url="/docs")


@router.get("/state")
def cluster_state(request: Request) -> ClusterSnapshot:
    config: Config = request.app.state.config
    cluster: Cluster = request.app.state.cluster
    r = ClusterSnapshot(
        cluster_id=config.cluster_id,
        node_states=[nd for nd in cluster._cluster_state._node_states.values()],
        live_nodes=cluster._faulure_detector.live_nodes(),
        dead_nodes=cluster._faulure_detector.dead_nodes(),
    )
    return r


@router.put("/kv_set")
async def kv_set(request: Request, kv: KeyValue) -> Status:
    cluster: Cluster = request.app.state.cluster
    nd = cluster.self_node_state()
    nd.set(kv.key, kv.value)
    return Status(status="ok")


@router.delete("/kv_mark")
async def kv_mark(request: Request, k: Key) -> Status:
    cluster: Cluster = request.app.state.cluster
    nd = cluster.self_node_state()
    nd.delete(k.key)
    return Status(status="ok")


def make_app(
    name: str,
    gossip_port: int = 7001,
    initial_kv: dict[str, str] | None = None,
    seed_nodes: list[tuple[str, int]] | None = None,
) -> FastAPI:
    node = NodeId(name=name, gossip_advertise_addr=("127.0.0.1", gossip_port))
    config = Config(node_id=node, seed_nodes=seed_nodes or [])

    title = f"API for {node.long_name()} cluster {config.cluster_id}"
    app = FastAPI(title=title, lifespan=lifespan)
    app.state.config = config
    app.state.initial_kv = initial_kv or {}
    app.include_router(router)
    return app


async def main():
    app1 = make_app("node1", gossip_port=7000, initial_kv={"name": "node1"})
    app2 = make_app(
        "node2",
        gossip_port=7001,
        seed_nodes=[("127.0.0.1", 7000)],
        initial_kv={"name": "node2"},
    )
    app3 = make_app(
        "node3",
        gossip_port=7002,
        seed_nodes=[("127.0.0.1", 7000)],
        initial_kv={"name": "node3"},
    )

    server1 = uvicorn.Server(config=uvicorn.Config(app1, port=8000))
    server2 = uvicorn.Server(config=uvicorn.Config(app2, port=8001))
    server3 = uvicorn.Server(config=uvicorn.Config(app3, port=8002))

    async with asyncio.TaskGroup() as group:
        group.create_task(server1.serve())
        group.create_task(server2.serve())
        group.create_task(server3.serve())


if __name__ == "__main__":
    asyncio.run(main())
