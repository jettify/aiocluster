import time
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from enum import IntEnum
from typing import Self

from .protos.messages_pb2 import AddressPb
from .protos.messages_pb2 import NodeDigestPb
from .protos.messages_pb2 import NodeIdPb
from .protos.messages_pb2 import VersionStatusEnumPb

__all__ = (
    "Config",
    "NodeDigest",
    "NodeId",
    "VersionStatusEnum",
    "VersionedValue",
    "FailureDetectorConfig",
)


class VersionStatusEnum(IntEnum):
    SET = 0
    DELETED = 1
    DELETE_AFTER_TTL = 2

    def to_pb(self) -> VersionStatusEnumPb.ValueType:
        return VersionStatusEnumPb.Value(self.name)

    @classmethod
    def from_pb(cls, pb: VersionStatusEnumPb.ValueType) -> "VersionStatusEnum":
        return VersionStatusEnum(pb)


@dataclass
class VersionedValue:
    value: str
    version: int
    status: VersionStatusEnum
    status_change_ts: datetime

    def is_deleted(self) -> bool:
        return self.status in (
            VersionStatusEnum.DELETED,
            VersionStatusEnum.DELETE_AFTER_TTL,
        )


Address = tuple[str, int]


@dataclass(frozen=True, eq=True, slots=True)
class NodeId:
    name: str
    generation_id: int = field(default_factory=time.monotonic_ns)
    gossip_advertise_addr: Address = ("localhost", 7001)

    def to_pb(self) -> NodeIdPb:
        addr = AddressPb(
            host=self.gossip_advertise_addr[0],
            port=self.gossip_advertise_addr[1],
        )
        return NodeIdPb(
            name=self.name,
            generation_id=self.generation_id,
            gossip_advertise_addr=addr,
        )

    @classmethod
    def from_pb(cls, pb: NodeIdPb) -> Self:
        host, port = pb.gossip_advertise_addr.host, pb.gossip_advertise_addr.port
        return cls(pb.name, pb.generation_id, (host, port))

    def long_name(self) -> str:
        host, port = self.gossip_advertise_addr
        return f"{self.name}-{self.generation_id}-{host}:{port}"


@dataclass(frozen=True, eq=True, slots=True)
class FailureDetectorConfig:
    phi_threshhold: float = 8.0
    sampling_window_size: int = 1_000
    max_interval: timedelta = timedelta(seconds=10)
    initial_interval: timedelta = timedelta(seconds=5)
    dead_node_grace_period: timedelta = timedelta(hours=24)


@dataclass(frozen=True, eq=True, slots=True)
class Config:
    node_id: NodeId
    cluster_id: str = "default-cluster"
    gossip_interval: float = 1.0  # seconds
    gossip_count: int = 3  # number of gossip participants in each round
    seed_nodes: list[Address] = field(default_factory=list)
    marked_for_deletion_grace_period: int = 3600 * 2  # seconds
    failure_detector: FailureDetectorConfig = field(
        default_factory=FailureDetectorConfig,
    )
    max_payload_size: int = 65_507
    connect_timeout: float = 3.0
    read_timeout: float = 3.0
    write_timeout: float = 3.0
    max_concurrent_gossip: int = 32


@dataclass(frozen=True, eq=True, slots=True)
class NodeDigest:
    node_id: NodeId
    heartbeat: int
    last_gc_version: int
    max_version: int

    def to_pb(self) -> NodeDigestPb:
        return NodeDigestPb(
            node_id=self.node_id.to_pb(),
            heartbeat=self.heartbeat,
            last_gc_version=self.last_gc_version,
            max_version=self.max_version,
        )

    @classmethod
    def from_pb(cls, pb: NodeDigestPb) -> Self:
        node_id = NodeId.from_pb(pb.node_id)
        return cls(node_id, pb.heartbeat, pb.last_gc_version, pb.max_version)
