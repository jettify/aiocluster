import asyncio
import socket
from collections.abc import Callable

import pytest

from aiocluster import Cluster
from aiocluster import Config
from aiocluster import NodeId


async def _wait_for_key(
    cluster: Cluster,
    node_id: NodeId,
    key: str,
    expected: str,
) -> None:
    timeout: float = 2.0

    async def _poll() -> None:
        while True:
            snapshot = cluster.snapshot()
            node_state = snapshot.node_states.get(node_id)
            if node_state is not None:
                vv = node_state.get(key)
                if vv is not None and vv.value == expected:
                    return
            await asyncio.sleep(0.02)

    async with asyncio.timeout(timeout):
        await _poll()


@pytest.mark.asyncio
async def test_gossip_propagates_values(free_port: Callable[[], int]) -> None:
    port1 = free_port()
    port2 = free_port()

    node1 = NodeId("node1", 1, ("127.0.0.1", port1))
    node2 = NodeId("node2", 1, ("127.0.0.1", port2))

    config1 = Config(
        node_id=node1,
        gossip_interval=0.02,
        seed_nodes=[("127.0.0.1", port2)],
        cluster_id="test-cluster",
    )
    config2 = Config(
        node_id=node2,
        gossip_interval=0.02,
        seed_nodes=[("127.0.0.1", port1)],
        cluster_id="test-cluster",
    )

    cluster1 = Cluster(config1)
    cluster2 = Cluster(config2)

    async with cluster1, cluster2:
        cluster1.set("k1", "v1")
        await _wait_for_key(cluster2, node1, "k1", "v1")
