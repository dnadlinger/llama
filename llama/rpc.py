import argparse
import asyncio
import atexit
import contextlib
from typing import TYPE_CHECKING, Callable, Optional

from sipyco.asyncio_tools import atexit_register_coroutine
from sipyco.common_args import (
    bind_address_from_args,
    init_logger_from_args,
    simple_network_args,
    verbosity_args,
)
from sipyco.pc_rpc import Server

from .influxdb import InfluxDBPusher, influxdb_args, influxdb_pusher_from_args

def add_chunker_methods(interface, chan) -> None:
    setattr(interface, "get_latest_" + chan.name, chan.get_latest)
    setattr(interface, "get_new_" + chan.name, chan.get_new)


def run_simple_rpc_server(
    port: int,
    setup_args: Optional[Callable[[argparse.ArgumentParser], None]],
    interface_name: str,
    setup_interface: Callable[
        [argparse.Namespace, Optional[InfluxDBPusher], asyncio.AbstractEventLoop],
        object,
    ],
) -> None:
    parser = argparse.ArgumentParser()

    influxdb_args(parser)
    simple_network_args(parser, port)
    verbosity_args(parser)
    if setup_args:
        setup_args(parser)

    args = parser.parse_args()
    init_logger_from_args(args)

    loop = asyncio.get_event_loop()
    atexit.register(loop.close)

    influx_pusher = influxdb_pusher_from_args(args)
    if influx_pusher:
        t = asyncio.ensure_future(influx_pusher.run())

        def stop() -> None:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                loop.run_until_complete(t)

        atexit.register(stop)

    interface = setup_interface(args, influx_pusher, loop)

    # Provide a default ping() method, which ARTIQ calls regularly for
    # heartbeating purposes.
    if not hasattr(interface, "ping"):
        interface.ping = lambda: True

    rpc_server = Server({interface_name: interface}, builtin_terminate=True)
    loop.run_until_complete(rpc_server.start(bind_address_from_args(args), args.port))
    atexit_register_coroutine(rpc_server.stop)

    loop.run_until_complete(rpc_server.wait_terminate())
