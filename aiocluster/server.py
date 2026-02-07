import asyncio
from asyncio import StreamReader
from asyncio import StreamWriter
from collections.abc import Sequence
from datetime import timedelta
from logging import LoggerAdapter
from random import Random
from types import TracebackType
from typing import Self

from .entities import Address
from .entities import Config
from .entities import NodeId
from .failure_detector import FailureDetector
from .log import logger
from .protos.messages_pb2 import AckPb
from .protos.messages_pb2 import BadClusterPb
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
        self,
        config: Config,
        initial_key_values: dict[str, str] | None = None,
        rng: Random | None = None,
    ) -> None:
        self._server: asyncio.Server | None = None
        self._config = config
        self._server_task: asyncio.Task[None] | None = None
        self._ticker = Ticker(self._gossip_multiple, self._config.gossip_interval)
        self._ticker_task: asyncio.Task[None] | None = None

        self._cluster_state = ClusterState(seed_addrs=set(self._config.seed_nodes))
        self._faulure_detector = FailureDetector(config.failure_detector)
        name = self._config.node_id.long_name()
        self._log = LoggerAdapter(logger, extra={"node": name}, merge_extra=True)

        node_state = self.self_node_state()
        node_state.inc_heartbeat()
        initial_key_values = initial_key_values or {}

        for k, v in initial_key_values.items():
            node_state.set(k, v)

        self._prev_live_nodes = dict[NodeId, int]
        self._tg = asyncio.TaskGroup()
        self._rng: Random = Random() if rng is None else rng

    async def __aenter__(self) -> Self:
        await self._tg.__aenter__()
        await self._boot()
        return self

    async def __aexit__(
        self,
        et: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: TracebackType | None = None,
    ) -> bool | None:
        await self._tg.__aexit__(et, exc, tb)
        return None

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

    async def _gossip(self, host: str, port: int, node_label: str = "live") -> None:
        name = self._config.node_id.long_name()
        self._log.debug(f"Node [{name}] gossiping with {node_label} ({host}:{port}).")
        syn_packet = self._make_syn_msg()
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.write(add_msg_size(syn_packet.SerializeToString()))
            await writer.drain()

            raw_msg = await self._read_message(reader)
            packet = PacketPb.FromString(raw_msg)
            if packet.WhichOneof("msg") == "bad_cluster":
                self._log.warning(
                    f"Got uexpected gossip message from wrong "
                    f"cluster: {syn_packet.cluster_id}"
                )
            else:
                ack_packet = self._handle_synac_msg(packet)

                writer.write(add_msg_size(ack_packet.SerializeToString()))
                await writer.drain()

            writer.close()
            await writer.wait_closed()
        except (OSError, asyncio.IncompleteReadError, ValueError) as exc:
            self._log.debug(
                f"Node [{name}] gossip failed with {node_label} ({host}:{port}): {exc}"
            )
        except Exception as exc:
            self._log.exception(
                f"Node [{name}] gossip error with {node_label} ({host}:{port}): {exc}"
            )

    async def _gossip_multiple(self) -> None:
        addrs = []

        for node_id in self._cluster_state.nodes():
            if node_id == self._config.node_id:
                continue

            a = node_id.gossip_advertise_addr
            addrs.append((a[0], a[1]))

        dead_nodes = {
            n.gossip_advertise_addr for n in self._faulure_detector.dead_nodes()
        }
        live_nodes = {
            n.gossip_advertise_addr for n in self._faulure_detector.live_nodes()
        }
        peer_nodes = {
            n.gossip_advertise_addr
            for n in self._cluster_state._node_states
            if n != self._config.node_id
        }

        seed_nodes = set(self._config.seed_nodes)

        gossip_nodes, gossip_dead, gossip_seed = select_nodes_for_gossip(
            peer_nodes,
            live_nodes,
            dead_nodes,
            seed_nodes,
            rng=self._rng,
            gossip_count=self._config.gossip_count,
        )

        node_state = self.self_node_state()
        node_state.inc_heartbeat()
        period = timedelta(seconds=self._config.marked_for_deletion_grace_period)
        self._cluster_state.gc_marked_for_deletion(period)

        async with asyncio.TaskGroup() as tg:
            for host, port in gossip_nodes:
                tg.create_task(self._gossip(host, port, node_label="live"))
            if gossip_dead is not None:
                host, port = gossip_dead
                tg.create_task(self._gossip(host, port, node_label="dead"))
            if gossip_seed is not None:
                host, port = gossip_seed
                tg.create_task(self._gossip(host, port, node_label="seed"))

        self._update_node_liveness()

    async def _serve(self) -> None:
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def _read_message(self, reader: StreamReader) -> bytes:
        size_data = await reader.readexactly(4)
        mg_size = decode_msg_size(size_data)
        if mg_size <= 0 or mg_size > self._config.max_payload_size:
            raise ValueError(f"Invalid message size: {mg_size}")
        raw_message = await reader.readexactly(mg_size)
        return raw_message

    async def _handle_message(self, reader: StreamReader, writer: StreamWriter) -> None:
        self.self_node_state().inc_heartbeat()

        raw_msg = await self._read_message(reader)
        syn_packet = PacketPb.FromString(raw_msg)
        assert syn_packet.syn
        if syn_packet.cluster_id != self._config.cluster_id:
            packet = PacketPb(
                cluster_id=self._config.cluster_id, bad_cluster=BadClusterPb()
            )
            writer.write(add_msg_size(packet.SerializeToString()))
            await writer.drain()
        else:
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
        if node_id == self.self_node_id:
            return
        node_state = self._cluster_state.node_state_or_default(node_id)
        if node_state.apply_heartbeat(heartbeat_value):
            self._faulure_detector.report_heartbeat(node_id)

    def _update_node_liveness(self) -> None:
        for node_id in self._cluster_state.nodes():
            if node_id == self.self_node_id:
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
        self._log.debug(
            f"Booting node {self.self_node_id.long_name()} for cluster "
            f"[{self._config.cluster_id}]"
        )
        host, port = self._config.node_id.gossip_advertise_addr
        server = await asyncio.start_server(self._handle_message, host, port)
        self._server = server
        self._server_task = self._tg.create_task(self._serve())
        self._ticker_task = self._tg.create_task(self._ticker._tick())

    async def shutdown(self) -> None:
        self._log.debug(
            f"Shutting down node: {self.self_node_id.long_name()} "
            f"for cluster  [{self._config.cluster_id}]"
        )
        if self._server is not None:
            await self._ticker.stop()

            self._server.close()
            await self._server.wait_closed()
            self._server_task = None

    def self_node_state(self) -> NodeState:
        return self._cluster_state.node_state_or_default(self._config.node_id)

    @property
    def self_node_id(self) -> NodeId:
        return self._config.node_id

    def live_nodes(self) -> Sequence[NodeId]:
        return [self.self_node_id] + self._faulure_detector.live_nodes()

    def dead_nodes(self) -> Sequence[NodeId]:
        return self._faulure_detector.dead_nodes()


def select_dead_node_to_gossip_with(
    dead_nodes: set[Address],
    live_nodes_count: int,
    dead_nodes_count: int,
    rng: Random,
) -> Address | None:
    if not dead_nodes:
        return None
    selection_probability = dead_nodes_count / (live_nodes_count + 1)
    if selection_probability > rng.random():
        return rng.choice(list(dead_nodes))
    return None


def select_seed_node_to_gossip_with(
    seed_nodes: set[Address], live_nodes_count: int, dead_nodes_count: int, rng: Random
) -> Address | None:
    selection_probability: float

    if (live_nodes_count + dead_nodes_count) == 0:
        selection_probability = 1.0
    else:
        selection_probability = len(seed_nodes) / (live_nodes_count + dead_nodes_count)

    if live_nodes_count == 0 or rng.random() <= selection_probability:
        return rng.choice(list(seed_nodes)) if seed_nodes else None
    return None


def select_nodes_for_gossip(
    peer_nodes: set[Address],
    live_nodes: set[Address],
    dead_nodes: set[Address],
    seed_nodes: set[Address],
    rng: Random,
    gossip_count: int = 3,
) -> tuple[list[Address], Address | None, Address | None]:
    live_nodes_count = len(live_nodes)
    dead_nodes_count = len(dead_nodes)

    # Select nodes to gossip with
    # On startup, select from cluster nodes since we don't know any live node yet
    nodes_to_gossip = list(peer_nodes if live_nodes_count == 0 else live_nodes)
    nodes = rng.sample(nodes_to_gossip, min(gossip_count, len(nodes_to_gossip)))

    # Check if we've gossiped with a seed node
    has_gossiped_with_a_seed_node = any(node in seed_nodes for node in nodes)

    # select a dead node for potential gossip, may be it returns to cluster
    random_dead_node_opt = select_dead_node_to_gossip_with(
        dead_nodes, live_nodes_count, dead_nodes_count, rng
    )

    # select a seed node for potential gossip, prevents network partition
    random_seed_node_opt = (
        select_seed_node_to_gossip_with(
            seed_nodes, live_nodes_count, dead_nodes_count, rng
        )
        if not has_gossiped_with_a_seed_node or live_nodes_count < len(seed_nodes)
        else None
    )
    return nodes, random_dead_node_opt, random_seed_node_opt
