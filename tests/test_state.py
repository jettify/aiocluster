from datetime import datetime
from datetime import timedelta

from aiocluster.entities import NodeId
from aiocluster.entities import VersionedValue
from aiocluster.entities import VersionStatusEnum
from aiocluster.protos.messages_pb2 import DeltaPb
from aiocluster.protos.messages_pb2 import NodeDeltaPb
from aiocluster.state import ClusterState
from aiocluster.state import Delta
from aiocluster.state import Digest
from aiocluster.state import KeyValueUpdate
from aiocluster.state import NodeDelta
from aiocluster.state import NodeState
from aiocluster.state import Staleness
from aiocluster.state import staleness_score


def test_apply_delta_creates_node() -> None:
    node_id = NodeId("node1", 1, ("127.0.0.1", 7000))
    delta = Delta(
        node_deltas=[
            NodeDelta(
                node_id=node_id,
                from_version_excluded=0,
                last_gc_version=0,
                key_values=[
                    KeyValueUpdate(
                        key="k1",
                        value="v1",
                        version=1,
                        status=VersionStatusEnum.SET,
                    )
                ],
                max_version=1,
            )
        ]
    )

    state = ClusterState(seed_addrs=set())
    state.apply_delta(delta)

    node_state = state.node_state(node_id)
    assert node_state is not None
    vv = node_state.get("k1")
    assert vv is not None
    assert vv.value == "v1"


def test_apply_delta_respects_per_key_versions() -> None:
    node_id = NodeId("node1", 1, ("127.0.0.1", 7000))
    ns = NodeState(node_id)
    ns.set_with_version("a", "old", 10)
    ns.set_with_version("b", "old", 1)

    delta = NodeDelta(
        node_id=node_id,
        from_version_excluded=1,
        last_gc_version=0,
        key_values=[
            KeyValueUpdate(
                key="b",
                value="new",
                version=11,
                status=VersionStatusEnum.SET,
            )
        ],
        max_version=11,
    )

    ns.apply_delta(delta)

    vv = ns.get("b")
    assert vv is not None
    assert vv.value == "new"
    assert vv.version == 11


def make_node_state() -> NodeState:
    node_id = NodeId("node", 0, ("localhost", 7001))
    return NodeState(node_id, 0, {}, 0, 0)


def test_apply_heartbeat() -> None:
    node_state = make_node_state()
    assert node_state.apply_heartbeat(5) is False
    assert node_state.heartbeat == 5
    assert node_state.apply_heartbeat(3) is False
    assert node_state.heartbeat == 5
    assert node_state.apply_heartbeat(6) is True
    assert node_state.heartbeat == 6


def test_apply_delta_skips_old_or_gc_versions() -> None:
    node_state = make_node_state()
    node_state.max_version = 2
    node_state.last_gc_version = 2

    kv_old = KeyValueUpdate("k1", "v1", 1, VersionStatusEnum.SET)
    kv_gc = KeyValueUpdate("k2", "v2", 2, VersionStatusEnum.DELETE_AFTER_TTL)
    kv_new = KeyValueUpdate("k3", "v3", 3, VersionStatusEnum.SET)
    delta = NodeDelta(node_state.node, 0, 0, [kv_old, kv_gc, kv_new], 3)

    node_state.apply_delta(delta, ts=datetime(2024, 1, 1, 0, 0, 0))

    assert node_state.get("k1") is None
    assert node_state.get("k2") is None
    assert node_state.get("k3") is not None


def test_gc_marked_for_deletion_updates_last_gc_version() -> None:
    node_state = make_node_state()
    now = datetime(2024, 1, 1, 0, 0, 10)
    node_state.last_gc_version = 1

    node_state.key_values = {
        "keep": VersionedValue(
            "v1", 2, VersionStatusEnum.SET, now - timedelta(seconds=5)
        ),
        "delete": VersionedValue(
            "v2", 5, VersionStatusEnum.DELETED, now - timedelta(seconds=20)
        ),
        "wait": VersionedValue(
            "v3",
            3,
            VersionStatusEnum.DELETE_AFTER_TTL,
            now - timedelta(seconds=2),
        ),
    }

    node_state.gc_marked_for_deletion(grace_period=timedelta(seconds=10), ts=now)

    assert "delete" not in node_state.key_values
    assert "keep" in node_state.key_values
    assert "wait" in node_state.key_values
    assert node_state.last_gc_version == 5


def test_prepart_node() -> None:
    node_state = make_node_state()
    node_state.max_version = 5
    node_state.last_gc_version = 2

    delta = NodeDelta(node_state.node, 6, 0, [], 0)
    assert node_state._prepart_node(delta) is False

    delta = NodeDelta(node_state.node, 0, 0, [], 0)
    assert node_state._prepart_node(delta) is True

    node_state.max_version = 1
    node_state.last_gc_version = 5
    delta = NodeDelta(node_state.node, 0, 1, [], 0)
    assert node_state._prepart_node(delta) is True


def test_staleness_score() -> None:
    node_state = make_node_state()
    node_state.key_values = {
        "k1": VersionedValue("v1", 1, VersionStatusEnum.SET, datetime(2024, 1, 1)),
        "k2": VersionedValue("v2", 2, VersionStatusEnum.SET, datetime(2024, 1, 1)),
    }
    node_state.max_version = 2

    assert staleness_score(node_state, floor_version=2) is None

    score = staleness_score(node_state, floor_version=0)
    assert isinstance(score, Staleness)
    assert score.is_unknown is True
    assert score.num_stale_key_values == 2


def test_compute_partial_delta_respecting_mtu_trims() -> None:
    node_state = make_node_state()
    node_state.key_values = {
        "k1": VersionedValue("v1", 1, VersionStatusEnum.SET, datetime(2024, 1, 1)),
        "k2": VersionedValue("v2", 2, VersionStatusEnum.SET, datetime(2024, 1, 1)),
    }
    node_state.max_version = 2
    node_state.last_gc_version = 1

    cluster = ClusterState(seed_addrs=set())
    cluster.node_state_or_default(node_state.node).key_values = node_state.key_values
    cluster.node_state_or_default(node_state.node).max_version = node_state.max_version
    cluster.node_state_or_default(
        node_state.node
    ).last_gc_version = node_state.last_gc_version

    digest = Digest()

    kvs = [KeyValueUpdate("k1", "v1", 1, VersionStatusEnum.SET)]
    nd_pb_1 = NodeDeltaPb(
        node_id=node_state.node.to_pb(),
        from_version_excluded=0,
        last_gc_version=node_state.last_gc_version,
        max_version=node_state.max_version,
        key_values=[k.to_pb() for k in kvs],
    )
    size1 = DeltaPb(node_deltas=[nd_pb_1]).ByteSize()

    kvs2 = [
        KeyValueUpdate("k1", "v1", 1, VersionStatusEnum.SET),
        KeyValueUpdate("k2", "v2", 2, VersionStatusEnum.SET),
    ]
    nd_pb_2 = NodeDeltaPb(
        node_id=node_state.node.to_pb(),
        from_version_excluded=0,
        last_gc_version=node_state.last_gc_version,
        max_version=node_state.max_version,
        key_values=[k.to_pb() for k in kvs2],
    )
    size2 = DeltaPb(node_deltas=[nd_pb_2]).ByteSize()

    assert size2 > size1
    mtu = size1 + 1

    delta = cluster.compute_partial_delta_respecting_mtu(
        digest=digest,
        mtu=mtu,
        scheduled_for_deletion=set(),
    )

    assert len(delta.node_deltas) == 1
    assert len(delta.node_deltas[0].key_values) == 1
