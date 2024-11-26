from aioc.entities import Config
from aioc.entities import NodeId
from aioc.server import Cluster


def test_ctor() -> None:
    gossip = NodeId("test1", 1, ("127.0.0.1", 7000))
    config = Config(gossip)
    cluster = Cluster(config)
    assert cluster
