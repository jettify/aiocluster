import socket
from collections.abc import Callable

import pytest


@pytest.fixture
def free_port() -> Callable[[], int]:
    def _free_port() -> int:
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = int(sock.getsockname()[1])
        sock.close()
        return port

    return _free_port
