from aiocluster.state import Digest
from aiocluster.state import NodeDigest
from aiocluster.state import NodeId
from aiocluster.state import NodeState
from aiocluster.state import VersionedValue
from aiocluster.state import VersionStatusEnum
from aiocluster.utils import utc_now


def make_node_state(port: int = 7000) -> NodeState:
    node_id = NodeId("pytest", 0, "localhost:7001")
    node_state = NodeState(node_id, 0, {}, 1, 1)
    return node_state


def test_ctor() -> None:
    node_id = NodeId("test", 0, "localhost:7001")
    key = "key"
    key_values = {"key": VersionedValue("value", 1, VersionStatusEnum.SET, utc_now())}
    node_state = NodeState(node_id, 0, key_values, 1, 1)
    v = node_state.get(key)
    assert v is not None
    assert v.value == "value"


def test_node_set_delete() -> None:
    node_state = make_node_state()
    node_state.set("key_a", "val_b")
    node_state.delete("key_a")
    vv = node_state.get("key_a")
    assert vv is None


def test_node_set_delete_after_ttl_set() -> None:
    node_state = make_node_state()
    node_state.set("key_a", "val_b")
    node_state.delete_after_ttl("key_a")
    node_state.set("key_a", "val_b2")
    vv = node_state.get_versioned("key_a")
    assert vv is not None
    assert vv.status == VersionStatusEnum.SET
    assert vv.value == "val_b2"


def test_node_set_with_ttl() -> None:
    node_state = make_node_state()
    node_state.set_with_ttl("key_a", "val_b")
    vv = node_state.get_versioned("key_a")
    assert vv is not None
    assert vv.status == VersionStatusEnum.DELETE_AFTER_TTL
    assert vv.value == "val_b"


def test_digest() -> None:
    digest = Digest({})
    node1 = NodeId("pytest1", 0, "localhost:7001")
    node2 = NodeId("pytest2", 0, "localhost:7002")
    node3 = NodeId("pytest3", 0, "localhost:7003")
    digest.add_node(node1, 101, 1, 11)
    digest.add_node(node2, 102, 20, 12)
    digest.add_node(node3, 103, 0, 13)
    # TODO: test serialization
