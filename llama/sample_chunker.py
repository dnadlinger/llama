import logging

from asyncio import AbstractEventLoop, Future, get_event_loop
from typing import Callable, Generic, Iterable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar('T')


class SampleChunker(Generic[T]):
    """
    Divides up a stream of measurements into chunks of a certain length or
    spanning a certain wall clock duration, whichever limit is hit first.
    Clients can also query the most recent value as well as await the next one.

    This is useful where the goal is to provide consistent per-chunk statistics,
    but to also gracefully handle situations where measurements become available
    unusually slowly.
    """

    def __init__(self, name: str, bin_finished: Callable[[Iterable[T]], None],
                 target_bin_size: int, max_bin_duration_secs: float,
                 loop: AbstractEventLoop=None):
        """
        Initialise a new statistics accumulation channel.

        :param name: A human-readable name for the channel.
        :param bin_finished: A callback to invoke
        :param target_bin_size: The target number of samples per bin, after
            which the
        :param max_bin_duration_secs: The maximum wall clock duration of each
            bin. After it is reached, a bin is finished even if the target number
        :param loop: The asyncio event loop to use.
        """
        self.name = name
        self.bin_finished = bin_finished
        self.target_bin_size = target_bin_size
        self.max_bin_duration_secs = max_bin_duration_secs
        self._loop = loop if loop else get_event_loop()

        #: Data points in the current bin.
        self._points = []

        self._last_point = None

        #: List of asyncio.Futures waiting for a new value to arrive.
        self._waiting_for_values = []

        self._schedule_timeout()

    # FIXME: No type hints due to issues when exported via ARTIQ's pc_rpc.
    async def get_latest(self):
        """
        Get the latest available measurement value.

        Yields if no point has been pushed yet.
        """
        if not self._last_point:
            return await self.get_new()
        return self._last_point

    # FIXME: No type hints due to issues when exported via ARTIQ's pc_rpc.
    async def get_new(self):
        """
        Await the next measurement value to be pushed and return it.
        """
        f = Future()
        self._waiting_for_values.append(f)
        return await f

    def push(self, value: T) -> None:
        """
        Push a new measurement value, appending it to the current bin and
        notifying any :meth:`get_new` calls waiting on it.
        """
        self._points.append(value)
        self._last_point = value

        if len(self._points) == self.target_bin_size:
            self._finish_bin()

        for f in self._waiting_for_values:
            f.set_result(value)
        self._waiting_for_values.clear()

    def _finish_bin(self) -> None:
        assert self._points, "Cannot finish empty bin"

        self._timeout.cancel()

        self.bin_finished(self._points)

        self._points = []
        self._schedule_timeout()

    def _schedule_timeout(self) -> None:
        self._timeout = self._loop.call_later(self.max_bin_duration_secs,
                                              self._timeout_elapsed)

    def _timeout_elapsed(self) -> None:
        if self._points:
            pass
        else:
            logger.debug("No data for channel '%s' in last %s seconds.",
                         self.name, self.max_bin_duration_secs)
        self._schedule_timeout()
