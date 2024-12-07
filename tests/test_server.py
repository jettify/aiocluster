from random import Random

from aiocluster.entities import Address
from aiocluster.server import select_nodes_for_gossip


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
