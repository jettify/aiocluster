import asyncio

import pytest

from aiocluster.ticker import Ticker
from aiocluster.ticker import simple_timeout
from aiocluster.utils import add_msg_size
from aiocluster.utils import decode_msg_size


def test_add_decode_msg_size_roundtrip() -> None:
    payload = b"hello"
    packed = add_msg_size(payload)
    assert decode_msg_size(packed) == len(payload)
    assert packed[4:] == payload


def test_simple_timeout() -> None:
    assert simple_timeout(1.0, 0.0, 0.25) == pytest.approx(0.75)
    assert simple_timeout(1.0, 0.0, 2.0) == 0


@pytest.mark.asyncio
async def test_ticker_start_stop_runs_once() -> None:
    calls: list[int] = []

    async def coro() -> None:
        calls.append(1)
        if len(calls) >= 1:
            ticker._closing = True

    ticker = Ticker(coro, interval=0)
    ticker.start()
    await asyncio.sleep(0)
    await ticker.stop()

    assert calls
    assert ticker.closed
