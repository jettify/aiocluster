import asyncio
from asyncio import StreamReader
from asyncio import StreamWriter
from collections.abc import Sequence
from logging import LoggerAdapter

from .entities import Config
from .entities import NodeId
from .failure_detector import FailureDetector
from .log import logger
from .protos.messages_pb2 import AckPb
from .protos.messages_pb2 import NodeIdPb
from .protos.messages_pb2 import PacketPb
from .protos.messages_pb2 import SynAckPb
from .protos.messages_pb2 import SynPb
from .state import ClusterState
from .state import Delta
from .state import Digest
from .state import NodeState
from .ticker import Ticker
from .utils import add_msg_size
from .utils import decode_msg_size

__all__ = ("Cluster",)


class Cluster:
    def __init__(
        self, config: Config, initial_key_values: dict[str, str] | None = None
    ) -> None:
        self._server = None
        self._config = config
        self._server_task = None
        self._ticker = Ticker(self._gossip_multiple, self._config.gossip_interval)
        self._ticker_task = None

        self._cluster_state = ClusterState(seed_addrs=set(self._config.seed_nodes))
        self._faulure_detector = FailureDetector(config.failure_detector)
        name = self._config.node_id.long_name
        self._log = LoggerAdapter(logger, extra={"node": name}, merge_extra=True)

        node_state = self.self_node_state()
        node_state.inc_heartbeat()
        initial_key_values = initial_key_values or {}
        for k, v in initial_key_values.items():
            node_state.set(k, v)

        self._prev_live_nodes = dict[NodeId, int]
        self._tg = asyncio.TaskGroup()

    async def __aenter__(self):
        await self._tg.__aenter__()
        await self._boot()
        return self

    async def __aexit__(self, et, exc, tb):
        await self._tg.__aexit__(et, exc, tb)

    def _make_syn_msg(self) -> PacketPb:
        secheduled_for_deleteion = self._faulure_detector.scheduled_for_deletion_nodes()
        digest = self._cluster_state.compute_digest(set(secheduled_for_deleteion))
        syn = SynPb(digest=digest.to_pb())
        packet = PacketPb(cluster_id=self._config.cluster_id, syn=syn)
        return packet

    def _handle_syn_msg(self, packet: PacketPb) -> PacketPb:
        digest = Digest.from_pb(packet.syn.digest)
        for node_id, nd in digest.node_digests.items():
            self._report_heartbeat(node_id, nd.heartbeat)

        secheduled_for_deleteion = self._faulure_detector.scheduled_for_deletion_nodes()
        self_digest = self._cluster_state.compute_digest(set(secheduled_for_deleteion))
        delta = self._cluster_state.compute_partial_delta_respecting_mtu(
            digest=digest,
            mtu=self._config.max_payload_size,
            secheduled_for_deleteion=set(secheduled_for_deleteion),
        )
        synack = SynAckPb(digest=self_digest.to_pb(), delta=delta.to_pb())
        packet = PacketPb(cluster_id=self._config.cluster_id, synack=synack)
        return packet

    def _handle_synac_msg(self, packet: PacketPb) -> PacketPb:
        secheduled_for_deleteion = self._faulure_detector.scheduled_for_deletion_nodes()
        assert packet.synack
        digest = Digest.from_pb(packet.synack.digest)
        new_delta = Delta.from_pb(packet.synack.delta)

        for node_id, nd in digest.node_digests.items():
            self._report_heartbeat(node_id, nd.heartbeat)

        self._cluster_state.apply_delta(delta=new_delta)
        delta = self._cluster_state.compute_partial_delta_respecting_mtu(
            digest=digest,
            mtu=self._config.max_payload_size,
            secheduled_for_deleteion=set(secheduled_for_deleteion),
        )

        ack = AckPb(delta=delta.to_pb())
        packet = PacketPb(cluster_id=self._config.cluster_id, ack=ack)
        return packet

    def _handl_ack(self, packet: PacketPb) -> None:
        new_delta = Delta.from_pb(packet.ack.delta)
        self._cluster_state.apply_delta(delta=new_delta)

    async def _gossip(self, host: str, port: int) -> None:
        name = self._config.node_id.long_name()
        self._log.debug(f"Node [{name}] gossiping with ({host}:{port}).")
        syn_packet = self._make_syn_msg()
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.write(add_msg_size(syn_packet.SerializeToString()))
            await writer.drain()

            raw_msg = await self._read_message(reader)
            synac_packet = PacketPb.FromString(raw_msg)
            ack_packet = self._handle_synac_msg(synac_packet)

            writer.write(add_msg_size(ack_packet.SerializeToString()))
            await writer.drain()

            writer.close()
            await writer.wait_closed()
        except OSError:
            raise

    async def _gossip_multiple(self) -> None:
        addrs = []

        for node_id in self._cluster_state.nodes():
            if node_id == self._config.node_id:
                continue
            a = node_id.gossip_advertise_addr
            addrs.append((a[0], a[1]))

        for a in self._config.seed_nodes:
            addrs.append((a[0], a[1]))

        async with asyncio.TaskGroup() as tg:
            for host, port in addrs:
                tg.create_task(self._gossip(host, port))

        self._update_node_liveness()

    async def _serve(self) -> None:
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def _read_message(self, reader: StreamReader) -> bytes:
        size_data = await reader.readexactly(4)
        mg_size = decode_msg_size(size_data)
        raw_message = await reader.readexactly(mg_size)
        return raw_message

    async def _handle_message(self, reader: StreamReader, writer: StreamWriter) -> None:
        self.self_node_state().inc_heartbeat()
        # import pprint

        # pprint.pprint(self._cluster_state._node_states)

        raw_msg = await self._read_message(reader)
        syn_packet = PacketPb.FromString(raw_msg)
        assert syn_packet.syn

        synack = self._handle_syn_msg(syn_packet)
        writer.write(add_msg_size(synack.SerializeToString()))
        await writer.drain()

        raw_msg = await self._read_message(reader)
        packet = PacketPb.FromString(raw_msg)
        assert packet.ack
        self._handl_ack(packet)

        writer.close()
        await writer.wait_closed()

    def _report_heartbeat(self, node_id: NodeId, heartbeat_value: int) -> None:
        if node_id == self.self_node_id():
            return
        node_state = self._cluster_state.node_state_or_default(node_id)
        if node_state.apply_heartbeat(heartbeat_value):
            self._faulure_detector.report_heartbeat(node_id)

    def _update_node_liveness(self) -> None:
        for node_id in self._cluster_state.nodes():
            if node_id == self.self_node_id():
                continue
            self._faulure_detector.update_node_liveness(node_id)
        # TODO: add event for new nodes
        # current_live_nodes = [
        #     (node_id, self._cluster_state.node_state_or_default(node_id).max_version)
        #     for node_id in self.live_nodes
        # ]

        nodes = self._faulure_detector.garbage_collect()
        for node_id in nodes:
            self._cluster_state.remove_node(node_id)

    async def _boot(self) -> None:
        self._log.debug(f"Booting cluster: {self.self_node_id().long_name()}")
        host, port = self._config.node_id.gossip_advertise_addr
        server = await asyncio.start_server(self._handle_message, host, port)
        self._server = server
        self._server_task = self._tg.create_task(self._serve())
        self._ticker_task = self._tg.create_task(self._ticker._tick())

    async def shutdown(self) -> None:
        self._log.debug(f"Shutting down cluster: {self.self_node_id().long_name()}")
        if self._server is not None:
            await self._ticker.stop()

            self._server.close()
            await self._server.wait_closed()
            self._server_task = None

    def self_node_state(self) -> NodeState:
        return self._cluster_state.node_state_or_default(self._config.node_id)

    def self_node_id(self) -> NodeId:
        return self._config.node_id

    def live_nodes(self) -> Sequence[NodeId]:
        return [self.self_node_id()] + self._faulure_detector.live_nodes()

    def dead_nodes(self) -> Sequence[NodeId]:
        return self._faulure_detector.dead_nodes()
