from aiocluster.entities import NodeId
from aiocluster.entities import VersionStatusEnum
from aiocluster.state import ClusterState
from aiocluster.state import Delta
from aiocluster.state import KeyValueUpdate
from aiocluster.state import NodeDelta
from aiocluster.state import NodeState


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
