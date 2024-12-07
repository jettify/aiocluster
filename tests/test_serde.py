from typing import Any

from google.protobuf.message import Message

from aiocluster.entities import NodeDigest
from aiocluster.entities import NodeId
from aiocluster.entities import VersionStatusEnum
from aiocluster.protos.messages_pb2 import DigestPb
from aiocluster.protos.messages_pb2 import KeyValueUpdatePb
from aiocluster.protos.messages_pb2 import NodeDeltaPb
from aiocluster.protos.messages_pb2 import NodeDigestPb
from aiocluster.protos.messages_pb2 import NodeIdPb
from aiocluster.protos.messages_pb2 import VersionStatusEnumPb
from aiocluster.state import Digest
from aiocluster.state import KeyValueUpdate
from aiocluster.state import NodeDelta


def assert_serialiation(message: Any, PbType: type[Message]) -> None:
    message_pb = message.to_pb()
    MessageType = message.__class__
    raw_bytes = message_pb.SerializeToString()
    pb = PbType.FromString(raw_bytes)
    deserialize_message = MessageType.from_pb(pb)
    assert message == deserialize_message


def test_version_status_enum() -> None:
    node_id = NodeId("foo", 0, ("localhost", 7001))
    assert_serialiation(node_id, NodeIdPb)


def test_node_digest() -> None:
    node_id = NodeId("foo", 0, ("localhost", 7001))
    node_digest = NodeDigest(node_id, 0, 0, 0)
    assert_serialiation(node_digest, NodeDigestPb)


def test_digest() -> None:
    node_id = NodeId("foo", 0, ("localhost", 7001))
    digest = Digest()
    digest.add_node(node_id, 0, 0, 0)
    assert_serialiation(digest, DigestPb)


def test_key_value_update() -> None:
    kvu = KeyValueUpdate("foo", "bar", 0, VersionStatusEnum.SET)
    assert_serialiation(kvu, KeyValueUpdatePb)


def test_node_delta() -> None:
    node_id = NodeId("foo", 0, ("localhost", 7001))
    kvu = KeyValueUpdate("foo", "bar", 0, VersionStatusEnum.SET)
    nd = NodeDelta(node_id, 0, 0, [kvu], 0)
    assert_serialiation(nd, NodeDeltaPb)
