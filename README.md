# aiocluster

[![GitHub Actions status for master branch](https://github.com/jettify/aiocluster/workflows/CI/badge.svg)](https://github.com/jettify/aiocluster/actions?query=workflow%3ACI)
[![Codecov](https://codecov.io/gh/jettify/aiocluster/branch/master/graph/badge.svg)](https://codecov.io/gh/jettify/aiocluster)
[![Python Versions](https://img.shields.io/pypi/pyversions/aiocluster.svg)](https://pypi.org/project/aiocluster)
[![PyPI Version](https://img.shields.io/pypi/v/aiocluster.svg)](https://pypi.python.org/pypi/aiocluster)

**aiocluster** is a Python 3.11+ library for building applications that run as a coordinated cluster. It helps services discover each other, keep shared state in sync, and handle node outages without custom coordination code.

## Features

- Easy cluster startup with seed nodes and a shared cluster ID.
- Automatic node discovery so members can join and leave dynamically.
- Built-in health monitoring to detect unreachable nodes.
- Gossip-based state sharing for lightweight cluster-wide metadata.
- Async-first API designed for `asyncio` applications.
- Hooks for reacting to cluster events in your application logic.
- Support for resilient, horizontally scaled service deployments.

## References

- ScuttleButt paper: <https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf>
- Phi Accrual error detection: <https://www.researchgate.net/publication/29682135_The_ph_accrual_failure_detector>
- chitchat <https://github.com/quickwit-oss/chitchat/blob/main/README.md>

## Requirements

- Python 3.11+.
- protobuf

## Usage

```python
import asyncio
import logging

from aiocluster import Cluster
from aiocluster import Config
from aiocluster import NodeId


async def main():
    node1_id = NodeId(name="simple1", gossip_advertise_addr=("127.0.0.1", 7000))
    node2_id = NodeId(name="simple2", gossip_advertise_addr=("127.0.0.1", 7001))
    node3_id = NodeId(name="simple3", gossip_advertise_addr=("127.0.0.1", 7002))

    config1 = Config(
        node_id=node1_id,
        gossip_interval=1,
        seed_nodes=[("127.0.0.1", 7002)],
        cluster_id="simple-aiocluster",
    )
    config2 = Config(
        node_id=node2_id,
        gossip_interval=1,
        seed_nodes=[("127.0.0.1", 7000)],
        cluster_id="simple-aiocluster",
    )
    config3 = Config(
        node_id=node3_id,
        gossip_interval=1,
        seed_nodes=[("127.0.0.1", 7001)],
        cluster_id="simple-aiocluster",
    )

    cluster1 = Cluster(config1, initial_key_values={"cluster": "1"})
    cluster2 = Cluster(config2, initial_key_values={"cluster": "2"})
    cluster3 = Cluster(config3, initial_key_values={"cluster": "3"})

    async with cluster1, cluster2, cluster3:
        await asyncio.sleep(10)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
```
