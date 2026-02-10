import asyncio
import gc
import shutil
import ssl
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from socket import socket

import pytest

from aiocluster import Cluster
from aiocluster import Config
from aiocluster import NodeId


def _run(*args: str) -> None:
    subprocess.run(
        args,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _write_san_config(path: Path, dns_name: str) -> None:
    path.write_text(
        "\n".join(
            [
                "[req]",
                "distinguished_name=req_distinguished_name",
                "req_extensions=v3_req",
                "prompt=no",
                "[req_distinguished_name]",
                f"CN={dns_name}",
                "[v3_req]",
                f"subjectAltName=DNS:{dns_name}",
            ]
        )
        + "\n"
    )


def _generate_ca(tmp: Path) -> tuple[Path, Path]:
    ca_key = tmp / "ca.key"
    ca_crt = tmp / "ca.crt"
    _run(
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-days",
        "3650",
        "-nodes",
        "-keyout",
        str(ca_key),
        "-out",
        str(ca_crt),
        "-subj",
        "/CN=aiocluster-test-ca",
    )
    return ca_key, ca_crt


def _generate_cert(
    tmp: Path, ca_key: Path, ca_crt: Path, name: str, dns_name: str
) -> tuple[Path, Path]:
    key = tmp / f"{name}.key"
    csr = tmp / f"{name}.csr"
    crt = tmp / f"{name}.crt"
    cfg = tmp / f"{name}.cnf"
    _write_san_config(cfg, dns_name)
    _run(
        "openssl",
        "req",
        "-new",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        str(key),
        "-out",
        str(csr),
        "-config",
        str(cfg),
    )
    _run(
        "openssl",
        "x509",
        "-req",
        "-in",
        str(csr),
        "-CA",
        str(ca_crt),
        "-CAkey",
        str(ca_key),
        "-CAcreateserial",
        "-out",
        str(crt),
        "-days",
        "3650",
        "-extensions",
        "v3_req",
        "-extfile",
        str(cfg),
    )
    return key, crt


CERTS_DIR = Path(__file__).parent / "certs"


@dataclass(frozen=True)
class CertBundle:
    ca_crt: Path
    node1_key: Path
    node1_crt: Path
    node2_key: Path
    node2_crt: Path


def _load_certs(certs_dir: Path) -> CertBundle:
    ca_crt = certs_dir / "ca.crt"
    node1_key = certs_dir / "node1.key"
    node1_crt = certs_dir / "node1.crt"
    node2_key = certs_dir / "node2.key"
    node2_crt = certs_dir / "node2.crt"

    for path in (ca_crt, node1_key, node1_crt, node2_key, node2_crt):
        if not path.exists():
            raise FileNotFoundError(path)

    _server_context(node1_crt, node1_key, ca_crt)
    _client_context(node1_crt, node1_key, ca_crt)
    _server_context(node2_crt, node2_key, ca_crt)
    _client_context(node2_crt, node2_key, ca_crt)

    return CertBundle(
        ca_crt=ca_crt,
        node1_key=node1_key,
        node1_crt=node1_crt,
        node2_key=node2_key,
        node2_crt=node2_crt,
    )


@pytest.fixture(scope="session")
def certs(tmp_path_factory: pytest.TempPathFactory) -> CertBundle:
    try:
        return _load_certs(CERTS_DIR)
    except (FileNotFoundError, ssl.SSLError, OSError, ValueError):
        tmp = tmp_path_factory.mktemp("certs")
        ca_key, ca_crt = _generate_ca(tmp)
        _generate_cert(tmp, ca_key, ca_crt, "node1", "node1.local")
        _generate_cert(tmp, ca_key, ca_crt, "node2", "node2.local")

        CERTS_DIR.mkdir(parents=True, exist_ok=True)
        for name in ("ca.crt", "node1.key", "node1.crt", "node2.key", "node2.crt"):
            shutil.copyfile(tmp / name, CERTS_DIR / name)

        return _load_certs(CERTS_DIR)


def _server_context(cert: Path, key: Path, ca: Path) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=str(cert), keyfile=str(key))
    ctx.load_verify_locations(cafile=str(ca))
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


def _client_context(cert: Path, key: Path, ca: Path) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_cert_chain(certfile=str(cert), keyfile=str(key))
    ctx.load_verify_locations(cafile=str(ca))
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


async def _wait_for(predicate: Callable[[], bool], interval: float = 0.05) -> bool:
    async def _poll() -> bool:
        while True:
            if predicate():
                return True
            await asyncio.sleep(interval)

    timeout: float = 2.0
    async with asyncio.timeout(timeout):
        return await _poll()


@pytest.mark.asyncio
async def test_mtls_identity_match(
    free_port: Callable[[], int], certs: CertBundle
) -> None:
    ca_crt = certs.ca_crt
    node1_key = certs.node1_key
    node1_crt = certs.node1_crt
    node2_key = certs.node2_key
    node2_crt = certs.node2_crt

    node1_server_ctx = _server_context(node1_crt, node1_key, ca_crt)
    node1_client_ctx = _client_context(node1_crt, node1_key, ca_crt)
    node2_server_ctx = _server_context(node2_crt, node2_key, ca_crt)
    node2_client_ctx = _client_context(node2_crt, node2_key, ca_crt)

    port1 = free_port()
    port2 = free_port()

    node1 = NodeId(
        "node1", gossip_advertise_addr=("127.0.0.1", port1), tls_name="node1.local"
    )
    node2 = NodeId(
        "node2", gossip_advertise_addr=("127.0.0.1", port2), tls_name="node2.local"
    )

    cfg1 = Config(
        node_id=node1,
        seed_nodes=[("127.0.0.1", port2)],
        gossip_interval=0.05,
        connect_timeout=0.5,
        read_timeout=0.5,
        write_timeout=0.5,
        tls_client_context=node1_client_ctx,
        tls_server_context=node1_server_ctx,
        tls_server_hostname="node2.local",
    )
    cfg2 = Config(
        node_id=node2,
        seed_nodes=[("127.0.0.1", port1)],
        gossip_interval=0.05,
        connect_timeout=0.5,
        read_timeout=0.5,
        write_timeout=0.5,
        tls_client_context=node2_client_ctx,
        tls_server_context=node2_server_ctx,
        tls_server_hostname="node1.local",
    )

    cluster1 = Cluster(cfg1)
    cluster2 = Cluster(cfg2)

    async with cluster1, cluster2:
        matched = await _wait_for(
            lambda: any(n.name == "node1" for n in cluster2.live_nodes())
        )
        assert matched


@pytest.mark.asyncio
async def test_mtls_identity_mismatch_is_rejected(
    free_port: Callable[[], int], certs: CertBundle
) -> None:
    ca_crt = certs.ca_crt
    node1_key = certs.node1_key
    node1_crt = certs.node1_crt
    node2_key = certs.node2_key
    node2_crt = certs.node2_crt

    node1_server_ctx = _server_context(node1_crt, node1_key, ca_crt)
    node1_client_ctx = _client_context(node1_crt, node1_key, ca_crt)
    node2_server_ctx = _server_context(node2_crt, node2_key, ca_crt)
    node2_client_ctx = _client_context(node2_crt, node2_key, ca_crt)

    port1 = free_port()
    port2 = free_port()

    node1 = NodeId(
        "node1",
        gossip_advertise_addr=("127.0.0.1", port1),
        tls_name="wrong.local",
    )
    node2 = NodeId(
        "node2", gossip_advertise_addr=("127.0.0.1", port2), tls_name="node2.local"
    )

    cfg1 = Config(
        node_id=node1,
        seed_nodes=[("127.0.0.1", port2)],
        gossip_interval=0.05,
        connect_timeout=0.5,
        read_timeout=0.5,
        write_timeout=0.5,
        tls_client_context=node1_client_ctx,
        tls_server_context=node1_server_ctx,
        tls_server_hostname="node2.local",
    )
    cfg2 = Config(
        node_id=node2,
        seed_nodes=[("127.0.0.1", port1)],
        gossip_interval=0.05,
        connect_timeout=0.5,
        read_timeout=0.5,
        write_timeout=0.5,
        tls_client_context=node2_client_ctx,
        tls_server_context=node2_server_ctx,
        tls_server_hostname="node1.local",
    )

    cluster1 = Cluster(cfg1)
    cluster2 = Cluster(cfg2)

    async with cluster1, cluster2:
        with pytest.raises(TimeoutError):
            await _wait_for(
                lambda: any(n.name == "node1" for n in cluster2.live_nodes())
            )
