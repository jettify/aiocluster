# aiocluster

[![GitHub Actions status for master branch](https://github.com/jettify/aiocluster/workflows/CI/badge.svg)](https://github.com/jettify/aiocluster/actions?query=workflow%3ACI)
[![Codecov](https://codecov.io/gh/jettify/aiocluster/branch/master/graph/badge.svg)](https://codecov.io/gh/jettify/aiocluster)
[![Python Versions](https://img.shields.io/pypi/pyversions/aiocluster.svg)](https://pypi.org/project/aiocluster)
[![PyPI Version](https://img.shields.io/pypi/v/aiocluster.svg)](https://pypi.python.org/pypi/aiocluster)

**aiocluster** is a Python 3.11+ module for distributed systems management, enabling cluster membership tracking, failure detection, configuration sharing, and metadata management. It facilitates scalable and fault-tolerant application sharding through robust cluster coordination capabilities.

## References

- ScuttleButt paper: <https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf>
- Phi Accrual error detection: <https://www.researchgate.net/publication/29682135_The_ph_accrual_failure_detector>
- chitchat <https://github.com/quickwit-oss/chitchat/blob/main/README.md>

## Requirements

- Python 3.11+ (library uses `asyncio.TaskGroup` feature).
- protobuf
