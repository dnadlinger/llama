Laboratory Logging and Monitoring for ARTIQ
===========================================

Provides an easy way to integrate typical polling-based device measurements
into a laboratory setup with ARITQ/InfluxDB.

Typical data sources include laser spectrum analysers, multimeters,
embedded controllers for power supplies or temperature controllers, and
many others instruments producing one or more measurements (typically
as floating point numbers).

The user can define a number of "channels", to which points can be pushed
using a normal synchronous Python method call, for instance from a driver
continuously polling a set of hardware measurements. This library then
exposes the data in two formats very useful in the context of running a
laboratory using [ARTIQ](https://m-labs.hk/artiq):

 - As a [SiPyCo](https://git.m-labs.hk/m-labs/sipyco) RPC interface, for
   instance for integration as a controller into ARTIQ experiments. To
   solve the synchronisation/measurement freshness problem, two methods
   are provided for each channel: one which immediately returns the last
   acquired value, and another which waits for a new value to come in
   first.

 - Pushed to [InfluxDB](https://docs.influxdata.com/influxdb/v1/), in
   aggregated form. This way, the full sample rate achievable on the
   hardware can be utilised without discarding data, while only pushing
   mean/percentile statistics to the time series database at a reasonable
   reduced rate. The extra statistics can be very useful to judge setup
   health/stability over time, compared to just logging a long-term
   average.

This project is configured for use with [`uv`](https://docs.astral.sh/uv/),
though it can be `pip install`ed as usual.

The `poe` task runner is set up to format and lint the source code;
try `uv run poe fmt` and `uv run poe lint`.
