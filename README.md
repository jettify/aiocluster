# aiocluster

**aiocluster** is a Python 3.11+ module that can be used for cluster
membership, failure detection, sharing configuration, and extra metadata values.
It can be used to shard your application in a way that's scalable and fault tolerant.

## References

- ScuttleButt paper: <https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf>
- Phi Accrual error detection: <https://www.researchgate.net/publication/29682135_The_ph_accrual_failure_detector>
- chitchat <https://github.com/quickwit-oss/chitchat/blob/main/README.md>

## Requirements

- Python 3.11+ library uses `TaskGroup` feature.
- protobuf
