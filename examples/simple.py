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

        await cluster1.shutdown()
        await cluster2.shutdown()
        await cluster3.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
