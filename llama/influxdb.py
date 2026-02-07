from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

import aiohttp
import numpy as np

if TYPE_CHECKING:
    import argparse
    from collections.abc import Iterable, Mapping

logger = logging.getLogger(__name__)


def influxdb_args(parser: argparse.ArgumentParser) -> None:
    """Register command line arguments to configure an InfluxDB instance to push
    measurements to (see :func:`influxdb_pusher_from_args`).
    """
    parser.add_argument(
        "--influxdb-endpoint",
        default=None,
        help="InfluxDB write endpoint to push data to (e,g. "
        "http://localhost:8086/write?db=mydb)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Connection timeout interval for InfluxDB (in seconds)",
    )
    parser.add_argument(
        "--retry-interval",
        type=int,
        default=60,
        help="Interval before retrying after a failed InfluxDB connection (seconds)",
    )
    parser.add_argument("--influxdb-tags", default=None)


def influxdb_pusher_from_args(args: argparse.Namespace) -> InfluxDBPusher:
    """Construct an :class:`InfluxDBPusher` from the standard arguments (see
    :func:`influxdb_args`), or `None` if not enabled.
    """
    if not args.influxdb_endpoint:
        logger.debug("No InfluxDB endpoint given, not exporting data.")
        return None

    if not args.influxdb_tags:
        msg = (
            "No InfluxDB tags set (--influxdb-tags). Refusing to push data to avoid "
            "later disambiguation/discoverability problems."
        )
        raise ValueError(msg)

    return InfluxDBPusher(
        args.influxdb_endpoint,
        args.influxdb_tags,
        args.timeout,
        args.retry_interval,
    )


def aggregate_stats_default(values: Iterable[float]) -> dict[str, float]:
    data = np.array(values)
    return {
        "min": np.min(data),
        "p05": np.percentile(data, 5),
        "mean": np.mean(data),
        "p95": np.percentile(data, 95),
        "max": np.max(data),
    }


class InfluxDBPusher:
    """Pushes a series of measurements to InfluxDB in the background.

    This is intended for situations where InfluxDB logging is not critical for
    the application, which might have to respond to latency-sensitive foreground
    queries. Thus, the actual communication happens on a background coroutine
    (and using non-blocking HTTP calls), and failures are logged as warnings,
    but ignored.
    """

    def __init__(
        self,
        write_endpoint: str,
        tags: str,
        timeout: float = 60,
        retry_interval: float = 60,
    ) -> None:
        """Create a new exporter instance.

        :param write_endpoint: The url for the /write?db=… HTTP POST endpoint to
            push the data to.
        :param tags: The tags to apply to each data point.
        :param loop: The event loop to use (None for asyncio default).
        """
        self.write_endpoint = write_endpoint
        self.tags = tags
        self._queue = asyncio.Queue(128)
        self._timeout = aiohttp.ClientTimeout(
            connect=timeout,
            sock_connect=timeout,
            sock_read=timeout,
            total=None,
        )
        self._retry_interval = retry_interval

    def push(self, field: str, values: Mapping[str, Any]) -> None:
        """Enqueue a new data point to be pushed to InfluxDB.

        :param field: The field name to use. This is used as the ``measurement`` in
            InfluxDB parlance.
        :param values: A dictionary of value names/contents for the data point
            (by InfluxDB convention named "value" if only a single one).
        """
        try:
            self._queue.put_nowait((field, values, time.time()))
        except asyncio.QueueFull:
            logger.warning(
                "Error pushing '%s' to %s: Queue full; dropping "
                "point (network connection or server down/slow?)",
                field,
                self.write_endpoint,
            )

    async def run(self) -> None:
        """Run the loop that drains the measurement queue and pushes the values
        to InfluxDB. Meant to be run as a background coroutine.
        """
        while True:
            try:
                async with aiohttp.ClientSession(timeout=self._timeout) as client:
                    while True:
                        field, stats, timestamp = await self._queue.get()

                        values = ",".join(f"{k}={v}" for k, v in stats.items())
                        body = f"{field},{self.tags} {values} {round(timestamp * 1e9)}"

                        async with client.post(self.write_endpoint, data=body) as resp:
                            if resp.status != 204:
                                resp_body = (await resp.text()).strip()
                                logger.warning(
                                    "Error pushing '%s' to %s (HTTP %s): %s",
                                    body,
                                    self.write_endpoint,
                                    resp.status,
                                    resp_body,
                                )
                                break
            except asyncio.TimeoutError:
                logger.warning(
                    "Connection timed out. Trying again in %s seconds…",
                    self._retry_interval,
                )
                await asyncio.sleep(self._retry_interval)
