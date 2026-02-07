from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from typing import Self

from .entities import Address
from .entities import NodeDigest
from .entities import NodeId
from .entities import VersionedValue
from .entities import VersionStatusEnum
from .protos.messages_pb2 import DeltaPb
from .protos.messages_pb2 import DigestPb
from .protos.messages_pb2 import KeyValueUpdatePb
from .protos.messages_pb2 import NodeDeltaPb
from .utils import utc_now


@dataclass(frozen=True, slots=True, eq=True)
class KeyValueUpdate:
    key: str
    value: str
    version: int
    status: VersionStatusEnum

    def to_pb(self) -> KeyValueUpdatePb:
        return KeyValueUpdatePb(
            key=self.key,
            value=self.value,
            version=self.version,
            status=self.status.to_pb(),
        )

    @classmethod
    def from_pb(cls, pb: KeyValueUpdatePb) -> Self:
        return cls(pb.key, pb.value, pb.version, VersionStatusEnum.from_pb(pb.status))


@dataclass
class Digest:
    node_digests: dict[NodeId, NodeDigest] = field(default_factory=dict)

    def add_node(
        self,
        node_id: NodeId,
        heartbeat: int,
        last_gc_version: int,
        max_version: int,
    ) -> None:
        nd = NodeDigest(node_id, heartbeat, last_gc_version, max_version)
        self.node_digests[node_id] = nd

    def to_pb(self) -> DigestPb:
        vs = (n.to_pb() for n in self.node_digests.values())
        return DigestPb(node_digests=vs)

    @classmethod
    def from_pb(cls, pb: DigestPb) -> Self:
        digests = [NodeDigest.from_pb(v) for v in pb.node_digests]
        return cls({v.node_id: v for v in digests})


@dataclass
class NodeDelta:
    node_id: NodeId
    from_version_excluded: int
    last_gc_version: int
    key_values: Sequence[KeyValueUpdate]
    max_version: int | None

    def to_pb(self) -> NodeDeltaPb:
        return NodeDeltaPb(
            node_id=self.node_id.to_pb(),
            from_version_excluded=self.from_version_excluded,
            last_gc_version=self.last_gc_version,
            key_values=[v.to_pb() for v in self.key_values],
            max_version=self.max_version,
        )

    @classmethod
    def from_pb(cls, pb: NodeDeltaPb) -> Self:
        return cls(
            node_id=NodeId.from_pb(pb.node_id),
            from_version_excluded=pb.from_version_excluded,
            last_gc_version=pb.last_gc_version,
            key_values=[KeyValueUpdate.from_pb(v) for v in pb.key_values],
            max_version=pb.max_version,
        )


@dataclass
class Delta:
    node_deltas: list[NodeDelta]

    def to_pb(self) -> DeltaPb:
        return DeltaPb(node_deltas=[nd.to_pb() for nd in self.node_deltas])

    @classmethod
    def from_pb(cls, pb: DeltaPb) -> Self:
        return cls(node_deltas=[NodeDelta.from_pb(p) for p in pb.node_deltas])


@dataclass
class NodeState:
    node: NodeId
    heartbeat: int = 0
    key_values: dict[str, VersionedValue] = field(default_factory=dict)
    max_version: int = 0
    # TODO: listeners: list[str]
    last_gc_version: int = 0

    def get(self, key: str) -> VersionedValue | None:
        v = self.get_versioned(key)
        if v is not None and v.is_deleted():
            return None
        return v

    def get_versioned(self, key: str) -> VersionedValue | None:
        return self.key_values.get(key)

    def set_versioned(self, key: str, versioned_value: VersionedValue) -> None:
        self.max_version = max(self.max_version, versioned_value.version)
        if key in self.key_values:
            existing = self.key_values[key]
            if existing.version >= versioned_value.version:
                return
        # TODO:: trigger key change envent
        self.key_values[key] = versioned_value

    def set_with_version(self, key: str, value: str, version: int) -> None:
        vv = VersionedValue(value, version, VersionStatusEnum.SET, utc_now())
        self.set_versioned(key, vv)

    def set(self, key: str, value: str) -> None:
        new_version = self.max_version + 1
        vv = self.get_versioned(key)
        if vv is not None and vv.value == value and vv.status == VersionStatusEnum.SET:
            return
        self.set_with_version(key, value, new_version)

    def set_with_ttl(self, key: str, value: str) -> None:
        vv = self.get_versioned(key)
        if (
            vv is not None
            and vv.value == value
            and vv.status == VersionStatusEnum.DELETE_AFTER_TTL
        ):
            return
        new_version = self.max_version + 1
        vv = VersionedValue(
            value,
            new_version,
            VersionStatusEnum.DELETE_AFTER_TTL,
            utc_now(),
        )
        self.set_versioned(key, vv)

    def delete(self, key: str) -> None:
        vv = self.get_versioned(key)
        if vv is None:
            return
        assert vv is not None
        self.max_version += 1

        vv.status = VersionStatusEnum.DELETED
        vv.version = self.max_version
        vv.status_change_ts = utc_now()
        vv.value = ""

    def delete_after_ttl(self, key: str) -> None:
        vv = self.get_versioned(key)
        if vv is None:
            return
        self.max_version += 1
        vv.status = VersionStatusEnum.DELETE_AFTER_TTL
        vv.version = self.max_version
        vv.status_change_ts = utc_now()

    def stale_key_values(
        self,
        floor_version: int,
    ) -> Generator[tuple[str, VersionedValue], None, None]:
        for k, v in self.key_values.items():
            if v.version > floor_version:
                yield (k, v)

    def apply_delta(
        self,
        node_delta: NodeDelta,
        ts: datetime | None = None,
        on_key_change: Callable[
            [NodeId, str, VersionedValue | None, VersionedValue], None
        ]
        | None = None,
    ) -> None:
        now = ts if ts is not None else utc_now()
        if node_delta.last_gc_version > self.last_gc_version:
            self.last_gc_version = node_delta.last_gc_version
            # Drop keys that were GC'd by the sender.
            self.key_values = {
                k: v
                for k, v in self.key_values.items()
                if v.version > self.last_gc_version
            }
        for kv_update in node_delta.key_values:
            if kv_update.version <= self.max_version:
                continue
            existing = self.get_versioned(kv_update.key)
            if existing is not None and existing.version >= kv_update.version:
                continue

            if (
                kv_update.status
                in (VersionStatusEnum.DELETE_AFTER_TTL, VersionStatusEnum.DELETED)
                and kv_update.version <= self.last_gc_version
            ):
                continue

            vv = VersionedValue(
                kv_update.value,
                kv_update.version,
                kv_update.status,
                now,
            )
            old_vv = existing
            self.set_versioned(kv_update.key, vv)
            if on_key_change is not None:
                on_key_change(self.node, kv_update.key, old_vv, vv)
        if node_delta.max_version is not None:
            self.max_version = max(self.max_version, node_delta.max_version)

    def _prepart_node(self, node_delta: NodeDelta) -> bool:
        if node_delta.from_version_excluded > self.max_version:
            return False

        if self.max_version > node_delta.last_gc_version:
            return True

        return self.last_gc_version > node_delta.last_gc_version

    def digest(self) -> NodeDigest:
        nd = NodeDigest(
            self.node,
            self.heartbeat,
            self.last_gc_version,
            self.max_version,
        )
        return nd

    def gc_marked_for_deletion(
        self,
        grace_period: timedelta,
        ts: datetime | None = None,
    ) -> None:
        now = ts if ts is not None else utc_now()
        max_deleted_version = self.last_gc_version
        _remove_keys = set()
        for key, versioned_value in self.key_values.items():
            if (
                versioned_value.status == VersionStatusEnum.SET
                or now < versioned_value.status_change_ts + grace_period
            ):
                continue
            else:
                _remove_keys.add(key)
                max_deleted_version = max(versioned_value.version, max_deleted_version)

        self.key_values = {
            k: v for k, v in self.key_values.items() if k not in _remove_keys
        }
        self.last_gc_version = max_deleted_version

    def inc_heartbeat(self) -> int:
        self.heartbeat += 1
        return self.heartbeat

    def apply_heartbeat(self, value: int) -> bool:
        if self.heartbeat == 0:
            self.heartbeat = value
            return False
        if value > self.heartbeat:
            self.heartbeat = value
            return True
        return False


class ClusterState:
    def __init__(self, seed_addrs: set[Address]) -> None:
        self._node_states: dict[NodeId, NodeState] = {}
        self._seed_addrs: set[Address] = seed_addrs

    def node_state(self, node_id: NodeId) -> NodeState | None:
        return self._node_states.get(node_id)

    def node_state_or_default(self, node_id: NodeId) -> NodeState:
        return self._node_states.setdefault(node_id, NodeState(node_id))

    def nodes(self) -> Sequence[NodeId]:
        return tuple(v for v in self._node_states)

    def seed_addrs(self) -> Sequence[tuple[str, int]]:
        return tuple(v for v in self._seed_addrs)

    def remove_node(self, node_id: NodeId) -> None:
        self._node_states.pop(node_id, None)

    def apply_delta(
        self,
        delta: Delta,
        ts: datetime | None = None,
        on_key_change: Callable[
            [NodeId, str, VersionedValue | None, VersionedValue], None
        ]
        | None = None,
    ) -> None:
        now = ts if ts is not None else utc_now()
        for nd in delta.node_deltas:
            ns = self._node_states.setdefault(nd.node_id, NodeState(nd.node_id))
            ns.apply_delta(nd, now, on_key_change=on_key_change)

    def compute_digest(self, scheduled_for_deletion: set[NodeId]) -> Digest:
        return Digest(
            {
                g_id: nd.digest()
                for g_id, nd in self._node_states.items()
                if g_id not in scheduled_for_deletion
            },
        )

    def gc_marked_for_deletion(
        self,
        marked_for_deletion_grace_period: timedelta,
    ) -> None:
        for ns in self._node_states.values():
            ns.gc_marked_for_deletion(marked_for_deletion_grace_period)

    def compute_partial_delta_respecting_mtu(
        self,
        digest: Digest,
        mtu: int,
        secheduled_for_deleteion: set[NodeId],
    ) -> Delta:
        stale_nodes = []
        for node_id, node_state in self._node_states.items():
            if node_id in secheduled_for_deleteion:
                continue
            digest_last_gc_version, digest_max_version = 0, 0
            d = digest.node_digests.get(node_id)
            if d is not None:
                digest_last_gc_version = d.last_gc_version
                digest_max_version = d.max_version

            if node_state.max_version <= digest_max_version:
                continue

            should_reset = (digest_last_gc_version < node_state.last_gc_version) and (
                digest_max_version < node_state.last_gc_version
            )
            from_version_excluded = 0 if should_reset else digest_max_version

            if staleness_score(node_state, from_version_excluded) is not None:
                stale_nodes.append((node_id, node_state, from_version_excluded))

            # sorted_nodes = sorted(
            #    stale_nodes, key=lambda v: (not v.is_unknown, v.num_stale_key_values)
            # )
        node_deltas: list[NodeDelta] = []
        delta_pb = DeltaPb()
        for node_id, node_state, from_version_excluded in stale_nodes:
            stale_kvs = [
                KeyValueUpdate(k, v.value, v.version, v.status)
                for k, v in node_state.key_values.items()
                if v.version > from_version_excluded
            ]
            if not stale_kvs:
                continue

            # Preserve increasing version order for deterministic deltas.
            stale_kvs.sort(key=lambda kv: kv.version)

            selected_kvs: list[KeyValueUpdate] = []
            nd_pb = NodeDeltaPb(
                node_id=node_id.to_pb(),
                from_version_excluded=from_version_excluded,
                last_gc_version=node_state.last_gc_version,
                max_version=node_state.max_version,
            )

            for kv in stale_kvs:
                kv_pb = kv.to_pb()
                nd_pb.key_values.append(kv_pb)
                if DeltaPb(node_deltas=[*delta_pb.node_deltas, nd_pb]).ByteSize() > mtu:
                    nd_pb.key_values.pop()
                    break
                selected_kvs.append(kv)

            if selected_kvs:
                node_deltas.append(
                    NodeDelta(
                        node_id,
                        from_version_excluded,
                        node_state.last_gc_version,
                        selected_kvs,
                        node_state.max_version,
                    )
                )
                delta_pb.node_deltas.append(nd_pb)

            if delta_pb.ByteSize() >= mtu:
                break

        return Delta(node_deltas=node_deltas)


@dataclass
class Staleness:
    is_unknown: bool
    max_version: int
    num_stale_key_values: int


def staleness_score(node_state: NodeState, floor_version: int) -> Staleness | None:
    if node_state.max_version <= floor_version:
        return None
    is_unknown = floor_version == 0
    if is_unknown:
        num_stale_key_values = len(node_state.key_values)
    else:
        num_stale_key_values = len(list(node_state.stale_key_values(floor_version)))
    return Staleness(is_unknown, node_state.max_version, num_stale_key_values)
