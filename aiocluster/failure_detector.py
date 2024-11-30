import array
from datetime import datetime
from datetime import timedelta

from .entities import FailureDetectorConfig
from .entities import NodeId
from .utils import utc_now


class SamplingWindow:
    def __init__(
        self, window_size: int, max_interval: timedelta, prior_inerval: timedelta,
    ):
        self._intervals = BoundedArrayStats(window_size)
        self._last_heartbeat: datetime | None = None
        self._max_inteval: timedelta = max_interval
        self._prev_mean: float = prior_inerval.total_seconds()
        self._prev_weight: float = 5.0

    def _mean(self) -> float | None:
        _len = len(self._intervals)
        if _len == 0:
            return None
        _sum = self._intervals.sum()
        return (_sum + self._prev_weight * self._prev_mean) / (_len + self._prev_weight)

    def report_heartbeat(self, ts: datetime | None = None) -> None:
        now = ts if ts is not None else utc_now()
        if self._last_heartbeat is not None:
            interval = now - self._last_heartbeat
            if interval <= self._max_inteval:
                self._intervals.append(interval.total_seconds())
        self._last_heartbeat = now

    def reset(self) -> None:
        self._intervals.clear()

    def phi(self, ts: datetime | None = None) -> float | None:
        now = ts if ts is not None else utc_now()
        if self._last_heartbeat is None:
            return None
        interval_mean = self._mean()
        if interval_mean is None:
            return None

        last_heartbeat = self._last_heartbeat
        elapsed_time = (now - last_heartbeat).total_seconds()
        return elapsed_time / interval_mean


class FailureDetector:
    def __init__(self, config: FailureDetectorConfig) -> None:
        self._node_samples: dict[NodeId, SamplingWindow] = {}
        self._config = config
        self._live_nodes: set[NodeId] = set()
        self._dead_nodes: dict[NodeId, datetime] = {}

    def live_nodes(self) -> list[NodeId]:
        return [v for v in self._live_nodes]

    def dead_nodes(self) -> list[NodeId]:
        return [v for v in self._dead_nodes]

    def get_or_create_sampling_window(self, gossip_id: NodeId) -> SamplingWindow:
        return self._node_samples.setdefault(
            gossip_id,
            SamplingWindow(
                self._config.sampling_window_size,
                self._config.max_interval,
                self._config.initial_interval,
            ),
        )

    def report_heartbeat(self, gossip_id: NodeId, ts: datetime | None = None) -> None:
        sw = self.get_or_create_sampling_window(gossip_id)
        sw.report_heartbeat(ts=ts)

    def phi(self, gossip_id: NodeId, ts: datetime | None = None) -> float | None:
        sw = self._node_samples.get(gossip_id)
        if sw is None:
            return None
        return sw.phi(ts=ts)

    def update_node_liveness(
        self, gossip_id: NodeId, ts: datetime | None = None,
    ) -> None:
        now = ts if ts is not None else utc_now()
        phi = self.phi(gossip_id, ts=now)
        is_alive = phi <= self._config.phi_threshhold if phi is not None else False
        if is_alive:
            self._live_nodes.add(gossip_id)
            self._dead_nodes.pop(gossip_id, None)
        else:
            self._live_nodes.discard(gossip_id)
            if gossip_id not in self._dead_nodes:
                self._dead_nodes[gossip_id] = now
            sw = self._node_samples.get(gossip_id)
            if sw is not None:
                sw.reset()

    def garbage_collect(self, ts: datetime | None = None) -> list[NodeId]:
        result = []
        now = ts if ts is not None else utc_now()

        for gossip_id, time_of_death in self._dead_nodes.items():
            if now >= time_of_death + self._config.dead_node_grace_period:
                result.append(gossip_id)

        for gossip_id in result:
            del self._dead_nodes[gossip_id]
            del self._node_samples[gossip_id]
        return result

    def scheduled_for_deletion_nodes(self, ts: datetime | None = None) -> list[NodeId]:
        now = ts if ts is not None else utc_now()
        result = []
        half_period = self._config.dead_node_grace_period / 2.0
        for gossip_id, time_of_death in self._dead_nodes.items():
            if now >= time_of_death + half_period:
                result.append(gossip_id)
        return result


class BoundedArrayStats:
    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._values = array.array("d", [0.0] * capacity)
        self._sum: float = 0.0
        self._index: int = 0
        self._is_filled: bool = False

    def append(self, interval: float) -> None:
        if self._is_filled:
            self._sum -= self._values[self._index]
        self._values[self._index] = interval
        self._sum += interval

        if self._index == self._capacity - 1:
            self._is_filled = True
            self._index = 0
        else:
            self._index += 1

    def sum(self) -> float:
        return self._sum

    def clear(self) -> None:
        self._sum = 0.0
        self._index = 0
        self._is_filled = False

    def __len__(self) -> int:
        if self._is_filled:
            return len(self._values)
        return self._index
