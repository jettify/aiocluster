import asyncio
from collections.abc import Awaitable
from collections.abc import Callable


class Ticker:
    def __init__(
        self,
        corofunc: Callable[[], Awaitable[None]],
        interval: float,
        timeout_func: Callable[[float, float, float], float] | None = None,
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self._interval = interval
        self._ticker_task: asyncio.Task[None] | None = None
        self._closing = False
        self._ticker = corofunc
        self._timout_func = timeout_func or simple_timeout
        self._on_error = on_error

    @property
    def closed(self) -> bool:
        return self._ticker_task is None

    async def _tick(self) -> None:
        loop = asyncio.get_event_loop()
        # TODO: add initial wait time to prevent all task to start
        # in same time
        while not self._closing:
            t_start = loop.time()

            try:
                await self._ticker()
            except Exception as e:
                if self._on_error is not None:
                    self._on_error(e)
                else:
                    raise

            t_stop = loop.time()
            t = self._timout_func(self._interval, t_start, t_stop)
            await asyncio.sleep(t)

    def start(self) -> None:
        self._ticker_task = asyncio.create_task(self._tick())

    async def stop(self) -> None:
        self._closing = True
        if self._ticker_task is None:
            return
        await self._ticker_task
        self._ticker_task = None


def create_ticker(corofunc: Callable[[], Awaitable[None]], interval: float) -> Ticker:
    ticker = Ticker(corofunc, interval)
    ticker.start()
    return ticker


def simple_timeout(interval: float, tick_start: float, tick_stop: float) -> float:
    t = max(interval - (tick_stop - tick_start), 0)
    return t
