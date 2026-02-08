from collections import deque
from datetime import datetime
from datetime import timedelta
from random import Random

import pytest

from aiocluster import FailureDetectorConfig
from aiocluster import NodeId
from aiocluster.entities import FailureDetectorConfig
from aiocluster.entities import NodeId
from aiocluster.failure_detector import BoundedArrayStats
from aiocluster.failure_detector import FailureDetector
from aiocluster.failure_detector import SamplingWindow
from aiocluster.utils import utc_now


@pytest.fixture
def rng() -> Random:
    seed = 1234
    return Random(seed)


def test_bounded_array() -> None:
    capacity = 5
    arr = BoundedArrayStats(capacity)
    expected: deque[float] = deque(maxlen=capacity)

    for i in range(1, capacity):
        assert len(arr) < capacity
        assert not arr._is_filled
        arr.append(i * 0.1)
        expected.append(i * 0.1)
        assert len(arr) == i
        assert arr.sum() == sum(expected)

    assert not arr._is_filled

    for i in range(capacity):
        arr.append(i * 0.1)
        expected.append(i * 0.1)

        assert arr._is_filled
        assert len(arr) == capacity
        assert len(expected) == capacity
        assert arr.sum() == sum(expected)


def advance(t: datetime, delta: timedelta) -> datetime:
    return t + delta


def test_sampling_window() -> None:
    sw = SamplingWindow(10, timedelta(seconds=5), timedelta(seconds=2))
    now = utc_now()
    sw.report_heartbeat(now)

    t1 = now + timedelta(seconds=3)
    sw.report_heartbeat(t1)

    mean = (3.0 + 2.0 * 5.0) / (1.0 + 5.0)
    assert sw.phi(t1) == pytest.approx(0.0)

    t2 = t1 + timedelta(seconds=1)
    assert sw.phi(t2) == pytest.approx(1.0 / mean)

    t3 = t2 + timedelta(seconds=5)
    sw.report_heartbeat(t3)
    t4 = t3 + timedelta(seconds=2)

    assert sw.phi(t4) == pytest.approx(2.0 / mean)
    t5 = t4 + timedelta(seconds=100)
    sw.reset()
    sw.report_heartbeat(t5)
    assert sw.phi(now) is None
    t6 = t5 + timedelta(seconds=2)
    sw.report_heartbeat(t6)
    t7 = t6 + timedelta(seconds=4)
    new_mean = (2.0 + 2.0 * 5.0) / (1.0 + 5.0)
    assert sw.phi(t7) == pytest.approx(4.0 / new_mean)


def test_failure_detector_does_not_see_a_node_as_alive_with_single_heartbeat() -> None:
    config = FailureDetectorConfig()
    fd = FailureDetector(config)
    node_id = NodeId("pytest", 0, ("localhost", 7001))

    fd.report_heartbeat(node_id)
    fd.update_node_liveness(node_id)
    assert node_id in fd.dead_nodes()
    assert len(fd.dead_nodes())
    assert node_id not in fd.live_nodes()


def test_failure_detector(rng: Random) -> None:
    t = utc_now()
    config = FailureDetectorConfig()
    fd = FailureDetector(config)

    nodes = [
        NodeId("pytest", 1, ("localhost", 7001)),
        NodeId("pytest", 2, ("localhost", 7002)),
        NodeId("pytest", 3, ("localhost", 7003)),
    ]

    for _ in range(100):
        node_id = rng.choice(nodes)
        t = advance(t, timedelta(seconds=1))
        fd.report_heartbeat(node_id, ts=t)

    for node_id in nodes:
        fd.update_node_liveness(node_id, ts=t)

    assert len(fd.live_nodes()) == 3
    assert len(fd.dead_nodes()) == 0

    t = advance(t, timedelta(seconds=50))
    for node_id in nodes:
        fd.update_node_liveness(node_id, ts=t)
    assert len(fd.live_nodes()) == 0
    assert len(fd.dead_nodes()) == 3

    removed_nodes = fd.garbage_collect(t)
    assert len(removed_nodes) == 0

    t = advance(t, timedelta(seconds=25 * 3600))
    removed_nodes = fd.garbage_collect(t)
    assert len(removed_nodes) == 3
    assert len(fd.dead_nodes()) == 0
    assert len(fd.live_nodes()) == 0


def test_bounded_array_stats_rollover_and_clear() -> None:
    stats = BoundedArrayStats(capacity=2)
    stats.append(1.0)
    stats.append(2.0)
    stats.append(3.0)

    assert len(stats) == 2
    assert stats.sum() == 5.0

    stats.clear()
    assert len(stats) == 0
    assert stats.sum() == 0.0


def test_sampling_window_respects_max_interval() -> None:
    sw = SamplingWindow(
        window_size=2,
        max_interval=timedelta(seconds=1),
        prior_interval=timedelta(seconds=1),
    )
    t0 = datetime(2024, 1, 1, 0, 0, 0)
    sw.report_heartbeat(ts=t0)
    sw.report_heartbeat(ts=t0 + timedelta(seconds=2))

    assert sw.phi(ts=t0 + timedelta(seconds=2)) is None

    sw.report_heartbeat(ts=t0 + timedelta(seconds=2, milliseconds=500))
    phi = sw.phi(ts=t0 + timedelta(seconds=3))
    assert phi is not None


def test_failure_detector_garbage_collect_and_scheduled_nodes() -> None:
    cfg = FailureDetectorConfig(dead_node_grace_period=timedelta(seconds=10))
    fd = FailureDetector(cfg)
    node_id = NodeId("n1", 0, ("localhost", 7001))

    now = datetime(2024, 1, 1, 0, 0, 0)
    fd.report_heartbeat(node_id, ts=now)
    fd.update_node_liveness(node_id, ts=now)

    scheduled = fd.scheduled_for_deletion_nodes(ts=now + timedelta(seconds=5))
    assert node_id in scheduled

    removed = fd.garbage_collect(ts=now + timedelta(seconds=11))
    assert removed == [node_id]
