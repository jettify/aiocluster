import struct
from asyncio import StreamReader
from random import Random

import pytest

from aiocluster.entities import Address
from aiocluster.entities import Config
from aiocluster.entities import NodeId
from aiocluster.server import Cluster
from aiocluster.server import select_dead_node_to_gossip_with
from aiocluster.server import select_nodes_for_gossip
from aiocluster.server import select_seed_node_to_gossip_with


def test_select_nodes_for_gossip() -> None:
    node1: Address = ("127.0.0.1", 7000)
    node2: Address = ("127.0.0.1", 7001)
    node3: Address = ("127.0.0.1", 7002)
    peers = {node1, node2, node3}
    live = {node1, node2}
    dead = {node3}
    seed = {node2}
    rng = Random(1)
    nodes, dead_node, seed_node = select_nodes_for_gossip(peers, live, dead, seed, rng)
    assert len(nodes) == 2
    assert dead_node is not None
    assert seed_node is None


class FakeReader(StreamReader):
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = list(chunks)

    async def readexactly(self, n: int) -> bytes:
        return self._chunks.pop(0)


def test_select_dead_node_to_gossip_with() -> None:
    rng = Random(0)
    assert select_dead_node_to_gossip_with(set(), 1, 0, rng) is None

    dead_nodes = {("localhost", 7001)}
    selected = select_dead_node_to_gossip_with(dead_nodes, 0, 1, rng)
    assert selected in dead_nodes


def test_select_seed_node_to_gossip_with() -> None:
    rng = Random(0)
    seeds = {("localhost", 7001)}
    selected = select_seed_node_to_gossip_with(seeds, 0, 0, rng)
    assert selected in seeds

    assert select_seed_node_to_gossip_with(set(), 1, 0, rng) is None


@pytest.mark.asyncio
async def test_cluster_read_message_validation() -> None:
    node_id = NodeId("node", 0, ("localhost", 7001))
    config = Config(node_id=node_id, max_payload_size=10)
    cluster = Cluster(config)

    reader = FakeReader([struct.pack(">I", 0)])
    with pytest.raises(ValueError):
        await cluster._read_message(reader)

    payload = b"abc"
    reader = FakeReader([struct.pack(">I", len(payload)), payload])
    assert await cluster._read_message(reader) == payload
