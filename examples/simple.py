import asyncio
import logging

from aiocluster import Cluster
from aiocluster import Config
from aiocluster import NodeId


async def main():
    node1 = NodeId(name="test1", gossip_advertise_addr=("127.0.0.1", 7000))
    node2 = NodeId(name="test2", gossip_advertise_addr=("127.0.0.1", 7001))
    node3 = NodeId(name="test3", gossip_advertise_addr=("127.0.0.1", 7002))

    config1 = Config(node_id=node1)
    config2 = Config(node_id=node2, seed_nodes=[("127.0.0.1", 7000)])
    config3 = Config(node_id=node3, seed_nodes=[("127.0.0.1", 7000)])

    cluster1 = Cluster(config1, initial_key_values={"cluster1": "1"})
    cluster2 = Cluster(config2, initial_key_values={"cluster2": "2"})
    cluster3 = Cluster(config3, initial_key_values={"cluster3": "3"})

    await cluster1.boot()
    await cluster2.boot()
    await cluster3.boot()

    await asyncio.sleep(30)

    await cluster1.shutdown()
    await cluster2.shutdown()
    await cluster3.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
