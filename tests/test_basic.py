import aiocluster as ac


def test_ctor() -> None:
    node = ac.NodeId("test1", 1, ("127.0.0.1", 7000))
    config = ac.Config(node)
    cluster = ac.Cluster(config)
    assert cluster
