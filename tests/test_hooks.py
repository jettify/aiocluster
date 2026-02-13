import asyncio
import time
from collections.abc import Callable

import pytest

from aiocluster import Cluster
from aiocluster import Config
from aiocluster import NodeId


@pytest.mark.asyncio
async def test_key_change_hook_runs_in_background(
    free_port: Callable[[], int],
) -> None:
    node_id = NodeId("node", 1, ("127.0.0.1", free_port()))
    config = Config(node_id=node_id, gossip_interval=0.1)
    cluster = Cluster(config)
    finished = asyncio.Event()

    async def on_key_change(*_: object) -> None:
        await asyncio.sleep(0.05)
        finished.set()

    cluster.on_key_change(on_key_change)

    async with cluster:
        started = time.monotonic()
        cluster.set("k1", "v1")
        elapsed = time.monotonic() - started
        assert elapsed < 0.02
        await asyncio.wait_for(finished.wait(), timeout=1.0)


@pytest.mark.asyncio
async def test_key_change_hook_drop_on_full_queue(
    free_port: Callable[[], int],
) -> None:
    node_id = NodeId("node", 1, ("127.0.0.1", free_port()))
    config = Config(
        node_id=node_id,
        gossip_interval=0.1,
        hook_queue_maxsize=1,
    )
    cluster = Cluster(config)
    release = asyncio.Event()
    calls: list[str] = []

    async def on_key_change(_: NodeId, key: str, *__: object) -> None:
        calls.append(key)
        await release.wait()

    cluster.on_key_change(on_key_change)

    async with cluster:
        cluster.set("k1", "v1")
        cluster.set("k2", "v2")
        cluster.set("k3", "v3")
        await asyncio.sleep(0.05)
        release.set()
        await asyncio.sleep(0.05)
        stats = cluster.hook_stats()
        assert stats.dropped >= 1
        assert calls
        assert calls[0] == "k1"
        assert len(calls) <= 2


@pytest.mark.asyncio
async def test_hook_worker_continues_after_callback_error(
    free_port: Callable[[], int],
) -> None:
    node_id = NodeId("node", 1, ("127.0.0.1", free_port()))
    config = Config(node_id=node_id, gossip_interval=0.1)
    cluster = Cluster(config)
    called = asyncio.Event()

    async def failing_hook(*_: object) -> None:
        raise RuntimeError("boom")

    async def succeeding_hook(*_: object) -> None:
        called.set()

    cluster.on_key_change(failing_hook)
    cluster.on_key_change(succeeding_hook)

    async with cluster:
        cluster.set("k1", "v1")
        await asyncio.wait_for(called.wait(), timeout=1.0)
        assert cluster.hook_stats().errors >= 1


@pytest.mark.asyncio
async def test_close_without_drain_cancels_hook_worker(
    free_port: Callable[[], int],
) -> None:
    node_id = NodeId("node", 1, ("127.0.0.1", free_port()))
    config = Config(
        node_id=node_id,
        gossip_interval=0.1,
        drain_hooks_on_shutdown=False,
        hook_shutdown_timeout=0.05,
    )
    cluster = Cluster(config)
    running = asyncio.Event()
    release = asyncio.Event()

    async def blocking_hook(*_: object) -> None:
        running.set()
        await release.wait()

    cluster.on_key_change(blocking_hook)
    await cluster.start()
    cluster.set("k1", "v1")
    await asyncio.wait_for(running.wait(), timeout=1.0)

    started = time.monotonic()
    await cluster.close()
    elapsed = time.monotonic() - started

    assert elapsed < 0.2
