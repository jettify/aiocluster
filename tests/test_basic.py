import asyncio

import pytest

import aiocluster as ac


def test_ctor() -> None:
    node = ac.NodeId("test1", 1, ("127.0.0.1", 7000))
    config = ac.Config(node)
    cluster = ac.Cluster(config)
    assert cluster


@pytest.mark.asyncio
async def test_start_stop() -> None:
    node_id = ac.NodeId("test1", 1, ("127.0.0.1", 7000))
    config = ac.Config(
        node_id=node_id,
        gossip_interval=0.01,
        cluster_id="simple-aiocluster",
    )
    cluster = ac.Cluster(config, initial_key_values={"cluster": "1"})
    async with cluster:
        await asyncio.sleep(0)
