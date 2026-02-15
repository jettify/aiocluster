import asyncio
import builtins
from asyncio import StreamReader
from asyncio import StreamWriter
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from logging import LoggerAdapter
from random import Random
from types import TracebackType
from typing import Self

from .entities import Address
from .entities import Config
from .entities import NodeId
from .entities import VersionedValue
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

__all__ = (
    "Cluster",
    "ClusterSnapshot",
    "HookStats",
    "NodeEventCallback",
    "KeyChangeCallback",
)

HookCallback = Callable[..., Awaitable[None]]
KeyChangeCallback = Callable[
    [NodeId, str, VersionedValue | None, VersionedValue], Awaitable[None]
]
NodeEventCallback = Callable[[NodeId], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class HookStats:
    enqueued: int
    processed: int
    dropped: int
    errors: int
    queue_size: int


@dataclass(frozen=True, slots=True)
class _HookEvent:
    callbacks: tuple[HookCallback, ...]
    args: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class ClusterSnapshot:
    cluster_id: str
    self_node_id: NodeId
    node_states: dict[NodeId, NodeState]
    live_nodes: list[NodeId]
    dead_nodes: list[NodeId]


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
        self._ticker = Ticker(
            self._gossip_multiple,
            self._config.gossip_interval,
            on_error=self._on_ticker_error,
        )

        self._cluster_state = ClusterState(seed_addrs=set(self._config.seed_nodes))
        self._failure_detector = FailureDetector(config.failure_detector)
        name = self._config.node_id.long_name()
        self._log = LoggerAdapter(logger, extra={"node": name}, merge_extra=True)

        node_state = self.self_node_state()
        node_state.inc_heartbeat()
        initial_key_values = initial_key_values or {}

        for k, v in initial_key_values.items():
            node_state.set(k, v)

        self._prev_live_nodes: set[NodeId] = set()
        self._on_node_join_callbacks: list[NodeEventCallback] = []
        self._on_node_leave_callbacks: list[NodeEventCallback] = []
        self._on_key_change_callbacks: list[KeyChangeCallback] = []
        if self._config.hook_queue_maxsize <= 0:
            msg = "hook_queue_maxsize must be > 0"
            raise ValueError(msg)
        self._hook_queue: asyncio.Queue[_HookEvent | None] = asyncio.Queue(
            maxsize=self._config.hook_queue_maxsize
        )
        self._hook_worker_task: asyncio.Task[None] | None = None
        self._hook_enqueued = 0
        self._hook_processed = 0
        self._hook_dropped = 0
        self._hook_errors = 0
        self._started = False
        self._closing = False
        self._gossip_semaphore = asyncio.Semaphore(
            max(1, self._config.max_concurrent_gossip)
        )
        self._rng: Random = Random() if rng is None else rng

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        et: type[BaseException] | None = None,
        exc: BaseException | None = None,
        tb: TracebackType | None = None,
    ) -> bool | None:
        await self.close()
        return None

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        await self._boot()

    async def close(self) -> None:
        if self._closing or not self._started:
            return
        self._closing = True
        await self._ticker.stop()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        if self._server_task is not None:
            self._server_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._server_task
            self._server_task = None
        self._server = None
        await self._stop_hook_worker()

    def hook_stats(self) -> HookStats:
        return HookStats(
            enqueued=self._hook_enqueued,
            processed=self._hook_processed,
            dropped=self._hook_dropped,
            errors=self._hook_errors,
            queue_size=self._hook_queue.qsize(),
        )

    def snapshot(self) -> ClusterSnapshot:
        return ClusterSnapshot(
            cluster_id=self._config.cluster_id,
            self_node_id=self.self_node_id,
            node_states=dict(self._cluster_state._node_states),
            live_nodes=self._failure_detector.live_nodes(),
            dead_nodes=self._failure_detector.dead_nodes(),
        )

    def on_node_join(self, callback: NodeEventCallback) -> None:
        self._on_node_join_callbacks.append(callback)

    def on_node_leave(self, callback: NodeEventCallback) -> None:
        self._on_node_leave_callbacks.append(callback)

    def on_key_change(self, callback: KeyChangeCallback) -> None:
        self._on_key_change_callbacks.append(callback)

    def get(self, key: str) -> str | None:
        vv = self.self_node_state().get(key)
        return None if vv is None else vv.value

    def get_versioned(self, key: str) -> VersionedValue | None:
        return self.self_node_state().get_versioned(key)

    def set(self, key: str, value: str) -> None:
        old_vv = self.get_versioned(key)
        self.self_node_state().set(key, value)
        new_vv = self.get_versioned(key)
        self._maybe_emit_key_change(key, old_vv, new_vv)

    def delete(self, key: str) -> None:
        old_vv = self.get_versioned(key)
        self.self_node_state().delete(key)
        new_vv = self.get_versioned(key)
        self._maybe_emit_key_change(key, old_vv, new_vv)

    def set_with_ttl(self, key: str, value: str) -> None:
        old_vv = self.get_versioned(key)
        self.self_node_state().set_with_ttl(key, value)
        new_vv = self.get_versioned(key)
        self._maybe_emit_key_change(key, old_vv, new_vv)

    def delete_after_ttl(self, key: str) -> None:
        old_vv = self.get_versioned(key)
        self.self_node_state().delete_after_ttl(key)
        new_vv = self.get_versioned(key)
        self._maybe_emit_key_change(key, old_vv, new_vv)

    def _emit_key_change(
        self,
        node_id: NodeId,
        key: str,
        old_vv: VersionedValue | None,
        new_vv: VersionedValue,
    ) -> None:
        self._enqueue_hook_event(
            callbacks=tuple(self._on_key_change_callbacks),
            args=(node_id, key, old_vv, new_vv),
        )

    def _maybe_emit_key_change(
        self,
        key: str,
        old_vv: VersionedValue | None,
        new_vv: VersionedValue | None,
    ) -> None:
        if new_vv is None:
            return
        if old_vv is None:
            self._emit_key_change(self.self_node_id, key, None, new_vv)
            return
        if (
            old_vv.version != new_vv.version
            or old_vv.status != new_vv.status
            or old_vv.value != new_vv.value
        ):
            self._emit_key_change(self.self_node_id, key, old_vv, new_vv)

    def _emit_node_join(self, node_id: NodeId) -> None:
        self._enqueue_hook_event(
            callbacks=tuple(self._on_node_join_callbacks),
            args=(node_id,),
        )

    def _emit_node_leave(self, node_id: NodeId) -> None:
        self._enqueue_hook_event(
            callbacks=tuple(self._on_node_leave_callbacks),
            args=(node_id,),
        )

    def _enqueue_hook_event(
        self, callbacks: tuple[HookCallback, ...], args: tuple[object, ...]
    ) -> None:
        if not callbacks:
            return
        event = _HookEvent(callbacks=callbacks, args=args)
        try:
            self._hook_queue.put_nowait(event)
            self._hook_enqueued += 1
        except asyncio.QueueFull:
            self._hook_dropped += 1

    async def _run_hook_worker(self) -> None:
        while True:
            hook_event = await self._hook_queue.get()
            if hook_event is None:
                self._hook_queue.task_done()
                return
            try:
                for callback in hook_event.callbacks:
                    try:
                        await callback(*hook_event.args)
                    except Exception as exc:
                        self._hook_errors += 1
                        self._log.exception(f"Hook callback error: {exc}")
            finally:
                self._hook_processed += 1
                self._hook_queue.task_done()

    async def _stop_hook_worker(self) -> None:
        if self._hook_worker_task is None:
            return
        task = self._hook_worker_task
        if self._config.drain_hooks_on_shutdown:
            try:
                await asyncio.wait_for(
                    self._hook_queue.join(),
                    timeout=self._config.hook_shutdown_timeout,
                )
            except TimeoutError:
                self._hook_dropped += self._hook_queue.qsize()
        else:
            self._hook_dropped += self._hook_queue.qsize()

        if task.done():
            with suppress(asyncio.CancelledError):
                await task
            self._hook_worker_task = None
            return

        if self._config.drain_hooks_on_shutdown:
            with suppress(asyncio.QueueFull):
                self._hook_queue.put_nowait(None)
            try:
                await asyncio.wait_for(task, timeout=self._config.hook_shutdown_timeout)
            except TimeoutError:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        else:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        self._hook_worker_task = None

    def _on_ticker_error(self, exc: Exception) -> None:
        self._log.exception(f"Ticker error: {exc}")

    def _make_syn_msg(self) -> PacketPb:
        scheduled_for_deletion = self._failure_detector.scheduled_for_deletion_nodes()
        digest = self._cluster_state.compute_digest(set(scheduled_for_deletion))
        syn = SynPb(digest=digest.to_pb())
        packet = PacketPb(cluster_id=self._config.cluster_id, syn=syn)
        return packet

    def _handle_syn_msg(self, packet: PacketPb) -> PacketPb:
        digest = Digest.from_pb(packet.syn.digest)
        for node_id, nd in digest.node_digests.items():
            self._report_heartbeat(node_id, nd.heartbeat)

        scheduled_for_deletion = self._failure_detector.scheduled_for_deletion_nodes()
        self_digest = self._cluster_state.compute_digest(set(scheduled_for_deletion))
        delta = self._cluster_state.compute_partial_delta_respecting_mtu(
            digest=digest,
            mtu=self._config.max_payload_size,
            scheduled_for_deletion=set(scheduled_for_deletion),
        )
        synack = SynAckPb(digest=self_digest.to_pb(), delta=delta.to_pb())
        packet = PacketPb(cluster_id=self._config.cluster_id, synack=synack)
        return packet

    def _handle_synac_msg(self, packet: PacketPb) -> PacketPb:
        scheduled_for_deletion = self._failure_detector.scheduled_for_deletion_nodes()
        assert packet.synack
        digest = Digest.from_pb(packet.synack.digest)
        new_delta = Delta.from_pb(packet.synack.delta)

        for node_id, nd in digest.node_digests.items():
            self._report_heartbeat(node_id, nd.heartbeat)

        self._cluster_state.apply_delta(
            delta=new_delta, on_key_change=self._emit_key_change
        )
        delta = self._cluster_state.compute_partial_delta_respecting_mtu(
            digest=digest,
            mtu=self._config.max_payload_size,
            scheduled_for_deletion=set(scheduled_for_deletion),
        )

        ack = AckPb(delta=delta.to_pb())
        packet = PacketPb(cluster_id=self._config.cluster_id, ack=ack)
        return packet

    def _handle_ack(self, packet: PacketPb) -> None:
        new_delta = Delta.from_pb(packet.ack.delta)
        self._cluster_state.apply_delta(
            delta=new_delta, on_key_change=self._emit_key_change
        )

    async def _gossip(
        self,
        host: str,
        port: int,
        node_label: str = "live",
        tls_name: str | None = None,
    ) -> None:
        name = self._config.node_id.long_name()
        self._log.debug(f"Node [{name}] gossiping with {node_label} ({host}:{port}).")
        syn_packet = self._make_syn_msg()
        writer: StreamWriter | None = None
        async with self._gossip_semaphore:
            try:
                if self._config.tls_client_context is None:
                    open_coro = asyncio.open_connection(host, port)
                else:
                    server_hostname = (
                        tls_name or self._config.tls_server_hostname or host
                    )
                    open_coro = asyncio.open_connection(
                        host,
                        port,
                        ssl=self._config.tls_client_context,
                        server_hostname=server_hostname,
                    )
                reader, writer = await asyncio.wait_for(
                    open_coro, timeout=self._config.connect_timeout
                )
                assert writer is not None
                await self._write_message(writer, syn_packet)

                raw_msg = await self._read_message(reader)
                packet = PacketPb.FromString(raw_msg)
                if packet.WhichOneof("msg") == "bad_cluster":
                    self._log.warning(
                        f"Got uexpected gossip message from wrong "
                        f"cluster: {syn_packet.cluster_id}"
                    )
                elif packet.WhichOneof("msg") == "synack" and packet.synack:
                    ack_packet = self._handle_synac_msg(packet)
                    await self._write_message(writer, ack_packet)
                else:
                    self._log.debug(
                        f"Node [{name}] unexpected gossip response from "
                        f"{node_label} ({host}:{port})."
                    )
            except (
                TimeoutError,
                OSError,
                asyncio.IncompleteReadError,
                ValueError,
            ) as exc:
                msg = f"[{name}] gossip failed with {node_label} ({host}:{port}): {exc}"
                self._log.debug(msg)
            except Exception as exc:
                msg = f"[{name}] gossip error with {node_label} ({host}:{port}): {exc}"
                self._log.exception(msg)
            finally:
                if writer is not None:
                    writer.close()
                    with suppress(Exception):
                        await writer.wait_closed()

    async def _gossip_multiple(self) -> None:
        tls_name_by_addr: dict[Address, str | None] = {
            node_id.gossip_advertise_addr: node_id.tls_name
            for node_id in self._cluster_state.nodes()
            if node_id != self._config.node_id
        }

        dead_nodes = {
            n.gossip_advertise_addr for n in self._failure_detector.dead_nodes()
        }
        live_nodes = {
            n.gossip_advertise_addr for n in self._failure_detector.live_nodes()
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
                tls_name = tls_name_by_addr.get((host, port))
                tg.create_task(
                    self._gossip(host, port, node_label="live", tls_name=tls_name)
                )
            if gossip_dead is not None:
                host, port = gossip_dead
                tls_name = tls_name_by_addr.get((host, port))
                tg.create_task(
                    self._gossip(host, port, node_label="dead", tls_name=tls_name)
                )
            if gossip_seed is not None:
                host, port = gossip_seed
                tls_name = tls_name_by_addr.get((host, port))
                tg.create_task(
                    self._gossip(host, port, node_label="seed", tls_name=tls_name)
                )

        self._update_node_liveness()

    async def _serve(self) -> None:
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def _read_message(self, reader: StreamReader) -> bytes:
        size_data = await asyncio.wait_for(
            reader.readexactly(4),
            timeout=self._config.read_timeout,
        )
        mg_size = decode_msg_size(size_data)
        if mg_size <= 0 or mg_size > self._config.max_payload_size:
            raise ValueError(f"Invalid message size: {mg_size}")
        raw_message = await asyncio.wait_for(
            reader.readexactly(mg_size),
            timeout=self._config.read_timeout,
        )
        return raw_message

    async def _write_message(self, writer: StreamWriter, packet: PacketPb) -> None:
        writer.write(add_msg_size(packet.SerializeToString()))
        await asyncio.wait_for(
            writer.drain(),
            timeout=self._config.write_timeout,
        )

    async def _handle_message(self, reader: StreamReader, writer: StreamWriter) -> None:
        self.self_node_state().inc_heartbeat()
        try:
            raw_msg = await self._read_message(reader)
            try:
                syn_packet = PacketPb.FromString(raw_msg)
            except Exception as exc:
                self._log.debug(f"Invalid gossip packet: {exc}")
                return

            if syn_packet.WhichOneof("msg") != "syn" or not syn_packet.syn:
                self._log.debug("Unexpected gossip message type.")
                return

            if not self._verify_peer_tls_name(syn_packet, writer):
                self._log.warning("TLS peer identity verification failed.")
                return

            if syn_packet.cluster_id != self._config.cluster_id:
                packet = PacketPb(
                    cluster_id=self._config.cluster_id, bad_cluster=BadClusterPb()
                )
                await self._write_message(writer, packet)
                return

            synack = self._handle_syn_msg(syn_packet)
            await self._write_message(writer, synack)

            raw_msg = await self._read_message(reader)
            try:
                packet = PacketPb.FromString(raw_msg)
            except Exception as exc:
                self._log.debug(f"Invalid gossip ack packet: {exc}")
                return
            if packet.WhichOneof("msg") != "ack" or not packet.ack:
                self._log.debug("Unexpected gossip ack message type.")
                return
            self._handle_ack(packet)
        except (TimeoutError, OSError, asyncio.IncompleteReadError, ValueError) as exc:
            self._log.debug(f"Server gossip error: {exc}")
        except Exception as exc:
            self._log.exception(f"Server gossip exception: {exc}")
        finally:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

    def _peer_cert_names(self, writer: StreamWriter) -> builtins.set[str]:
        sslobj = writer.get_extra_info("ssl_object")
        if sslobj is None:
            return set()
        peercert = writer.get_extra_info("peercert") or {}
        names: set[str] = set()
        for typ, value in peercert.get("subjectAltName", []):
            if typ in {"DNS", "IP Address"}:
                names.add(value)
        for subject in peercert.get("subject", []):
            for key, value in subject:
                if key == "commonName":
                    names.add(value)
        return names

    def _verify_peer_tls_name(self, packet: PacketPb, writer: StreamWriter) -> bool:
        if self._config.tls_server_context is None:
            return True
        cert_names = self._peer_cert_names(writer)
        if not cert_names:
            return True
        if packet.WhichOneof("msg") != "syn" or not packet.syn:
            return False
        digest = Digest.from_pb(packet.syn.digest)
        for node_id in digest.node_digests:
            if node_id.tls_name and node_id.tls_name in cert_names:
                return True
        return False

    def _report_heartbeat(self, node_id: NodeId, heartbeat_value: int) -> None:
        if node_id == self.self_node_id:
            return
        node_state = self._cluster_state.node_state_or_default(node_id)
        if node_state.apply_heartbeat(heartbeat_value):
            self._failure_detector.report_heartbeat(node_id)

    def _update_node_liveness(self) -> None:
        for node_id in self._cluster_state.nodes():
            if node_id == self.self_node_id:
                continue
            self._failure_detector.update_node_liveness(node_id)
        current_live_nodes = set(self._failure_detector.live_nodes())
        for node_id in current_live_nodes - self._prev_live_nodes:
            self._emit_node_join(node_id)
        for node_id in self._prev_live_nodes - current_live_nodes:
            self._emit_node_leave(node_id)
        self._prev_live_nodes = current_live_nodes

        nodes = self._failure_detector.garbage_collect()
        for node_id in nodes:
            self._cluster_state.remove_node(node_id)

    async def _boot(self) -> None:
        self._log.debug(
            f"Booting node {self.self_node_id.long_name()} for cluster "
            f"[{self._config.cluster_id}]"
        )
        host, port = self._config.node_id.gossip_advertise_addr
        server = await asyncio.start_server(
            self._handle_message,
            host,
            port,
            ssl=self._config.tls_server_context,
        )
        self._server = server
        self._server_task = asyncio.create_task(self._serve())
        self._hook_worker_task = asyncio.create_task(self._run_hook_worker())
        self._ticker.start()

    async def shutdown(self) -> None:
        await self.close()

    def self_node_state(self) -> NodeState:
        return self._cluster_state.node_state_or_default(self._config.node_id)

    @property
    def self_node_id(self) -> NodeId:
        return self._config.node_id

    def live_nodes(self) -> Sequence[NodeId]:
        return [self.self_node_id] + self._failure_detector.live_nodes()

    def dead_nodes(self) -> Sequence[NodeId]:
        return self._failure_detector.dead_nodes()


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
