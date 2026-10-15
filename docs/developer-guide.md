# AVI engine developer guide

This guide is for developers who change `smartmet-engine-avi`, or use it from a plugin.
The engine serves aviation weather messages (METAR, TAF, SIGMET, AIRMET, …) and the
stations and FIR areas they belong to, from the AviDB PostgreSQL/PostGIS database.

[CLAUDE.md](../CLAUDE.md) has the architecture summary; `cnf/avi.conf.sample` documents
the message type settings in its comments.

## Contents

1. [Building and testing](#1-building-and-testing)
2. [The API](#2-the-api)
3. [How a query is built](#3-how-a-query-is-built)
4. [Message types and time ranges](#4-message-types-and-time-ranges)
5. [Configuration](#5-configuration)
6. [In-memory message cache](#6-in-memory-message-cache)
7. [Compatibility](#7-compatibility)
8. [Known pitfalls](#8-known-pitfalls)

---

## 1. Building and testing

```bash
make
make test                                  # Boost.Test programs in test/, need the test database
make -C test engine.test && ./test/engine.test --log_level=message
```

The tests use a PostgreSQL test database: the `smartmet-test` host, or with `CI=1` a
local temporary database. `test/cnf/valid.conf.in` is turned into `valid.conf` with the
right host. `test/cache.cpp` runs the same queries with the in-memory cache (§6) against
that database; building needs `duckdb-devel`.

## 2. The API

`Engine` is a class of **virtual** methods that throw "AVI engine not available" by
default, so plugins can be built and loaded without the engine; `EngineImpl` overrides
them.

| Call | Returns |
|------|---------|
| `queryStations(options)` | Stations matching the location options. |
| `queryMessages(stationIds, options)` | Messages for the given stations. |
| `queryStationsAndMessages(options)` | Both in one query. |
| `joinStationAndMessageData(stations, messages)` | Merge the two result sets. |
| `queryRejectedMessages(options)` | Messages the ingest rejected (`avidb_rejected_messages`). |
| `queryFIRAreas()` | The FIR areas (`icao_fir_yhdiste`), loaded on first use and kept. |

`QueryOptions` combines:

* **`LocationOptions`**: station ids, ICAO codes (with prefix filters: fewer than four
  letters match as a prefix), bounding box, lon/lat with radius, WKT, place names,
  countries;
* **`TimeOptions`**: an observation time or a time range, time format and zone, and flags
  for SIGMET and TAF special cases;
* message types, the output format (TAC or IWXXM), the parameters (columns) to return,
  and result limits.

Results are `StationQueryData` (column values per station id) or `QueryData`.

## 3. How a query is built

`EngineImpl`:

1. **validates** everything against the database or the configuration: station ids,
   ICAO codes and filters, places, countries, WKTs (`validateWKTs` asks PostGIS), times,
   message types and parameters;
2. **builds the SQL** from `Column` / `Table` / `QueryTable` descriptions: the requested
   columns, the joins they need (`avidb_stations`, `avidb_messages`,
   `avidb_messages_types`, `avidb_messages_routes`, `latest_messages`), and the
   conditions. Every user-supplied string is quoted with the connection's `quote()`;
3. uses a **record-set CTE**: candidate rows are first selected by the indexed
   `message_time`, within `recordsetstarttimeoffsethours` before and
   `recordsetendtimeoffsethours` after the requested time, and only then restricted by each
   message type's own validity rule (§4);
4. loads the result into the result containers. Results are read through the small
   `IResult` interface (`ResultAccessor.h`), implemented for libpqxx (`PqxxResult.h`) and
   for the DuckDB cache, so the same loading code serves both.

Connections come from a pool (`postgis` settings).

## 4. Message types and time ranges

Each configured message type has a **time range type**, which decides when a message is
considered valid at the requested time:

| `timerangetype` | Validity |
|-----------------|----------|
| `validtime` | `valid_from` … `valid_to`. |
| `messagevalidtime` | `message_time` … `valid_to`; like `messagetime` when both valid times are NULL (NIL messages, TAF). |
| `messagetime` | `message_time` … `message_time + validityhours`. |
| `creationtime` | `creation_time` … `valid_to`. |

With **`latestmessage = true`**, only the latest valid message of each type (or group of
types, if several names are listed together) per station is returned; otherwise all valid
messages. `messirpatterns` (`messir_heading LIKE` patterns) split the latest-message
grouping further. Each type also has a **scope** (station, FIR or global), which decides
how it relates to the location options.

The Finnish METAR filter (`filter_FI_METARxxx`) drops duplicate Finnish METARs according
to its configuration.

## 5. Configuration

| Key | Meaning |
|-----|---------|
| `postgis` | Host, port, database, credentials and connection pool size. |
| `message.types` | The message types with `timerangetype`, `validityhours`, `latestmessage`, `messirpatterns`, scope and query restrictions. |
| `message.recordsetstarttimeoffsethours`, `recordsetendtimeoffsethours` | The CTE window around the requested time (required, > 0). It must be wide enough for every type's validity. |
| `message.filter_FI_METARxxx` | The Finnish METAR deduplication. |
| `message.cache.*` | The optional in-memory message cache (§6), disabled by default. |
| `disabled` | Optional, default false. When true, the engine loads in dummy mode (see below). |

**Dummy mode.** `engine_class_creator` returns the base `Engine` instead of `EngineImpl` when
the server gives it no configuration file name, or an empty one, or when the file sets
`disabled = true`. The base class needs no database and every query method throws
`AVI engine not available`. Only `disabled` is read in that case, so the rest of the file
may be missing. This lets plugins that look up the engine at startup (avi, wms) run on a
machine without the AVI database. An empty configuration file is not the same thing: the
full engine is then created and fails on the missing `postgis` settings. Setting
`disabled = true` in the server's `engines.avi` section is different again: the server
does not load `avi.so`, and plugins that require it fail at startup.

## 6. In-memory message cache

Requests for the latest messages at the current time (EDR and timeseries clients polling
the newest METARs and TAFs) all reach PostgreSQL with nearly the same query. The optional
cache answers them from an in-process copy of the recent data instead (`AviCache`).

**How it works.** A background thread opens an in-memory [DuckDB](https://duckdb.org)
database and copies into it the `avidb_messages` rows of the last `durationhours`, and the
whole `avidb_stations`, `avidb_message_types`, `avidb_message_format` and
`avidb_message_routes` tables. Every `updateintervalsec` it re-reads the messages
created or modified within the last `updateintervalsec + safetymarginsec` seconds (and
stations modified then), replaces those rows, reloads the small type, format and route
tables, and deletes messages older than `durationhours`. DuckDB runs the SQL the engine
generates for PostgreSQL almost unchanged, so a cached query is the very same SQL string
executed against the copy; DuckDB's MVCC gives each query a consistent snapshot while the
updater commits. All times are stored as UTC.

**Which queries use it.** `queryMessages` uses the cache when it is enabled, the initial
load has completed, and `EngineImpl::isCacheable` accepts the query: the observation time is
`current_timestamp` with no time range, the messages are accepted ones (not rejected), the
format is TAC or IWXXM, there is no route, and no parameter needs PostGIS (`longitude`,
`latitude`, `lonlat`, `latlon`, `distance`, `bearing`). Station lookups and validation
always use PostgreSQL. If a cached query fails for any reason (for example the SQL uses a
PostGIS function), the error is logged and the query is run against PostgreSQL. Requiring
`durationhours > recordsetstarttimeoffsethours` guarantees that the copy covers the
record-set window of every accepted query.

**Settings** (`message.cache`, all optional):

| Key | Default | Meaning |
|-----|---------|---------|
| `enabled` | `false` | Create and use the cache. |
| `durationhours` | 48 | Hours of messages kept; must be greater than `recordsetstarttimeoffsethours`. |
| `updateintervalsec` | 10 | Refresh interval, > 0. |
| `safetymarginsec` | 600 | Extra re-read window for late or updated rows. |
| `shadowcompare` | `false` | Run cacheable queries against both, log differences, return the PostgreSQL result. A rollout aid. |
| `memorylimitmb` | 0 | DuckDB `memory_limit` in MB; 0 keeps DuckDB's default (80% of RAM). |

**Disabled, it costs nothing.** With `enabled = false` no DuckDB database or thread is
created and every query runs against PostgreSQL as before. `libduckdb.so` is still linked,
so the `duckdb` package is always required.

**Startup and failures.** The initial load runs in the background, so the engine starts
without waiting; queries use PostgreSQL until it finishes. A failed load or refresh is
logged and retried on the next interval.

## 7. Compatibility

The public methods are virtual and plugins call them through the vtable: add new methods
**at the end** of `Engine` only. `QueryOptions`, `LocationOptions`, `TimeOptions` and the
result containers are built or read by the plugins (avi, timeseries, edr), so changing their
layout requires rebuilding those together with the engine. The same holds for the inline
`getValues` methods in `Engine.h`, which take an `IResult` since the cache was added.

## 8. Known pitfalls

* **A too narrow record-set window drops messages.** A message whose `message_time` is
  outside the CTE window is never considered, however long it is valid (a long-valid TAF
  or SIGMET). Keep the offsets at least as long as the longest validity.
* **Validation hits the database.** Station ids, ICAO codes, places, countries and WKTs are
  checked with database queries before the actual query runs.
* **Short ICAO strings are prefix matches.** `EF` means every station starting with `EF`.
* **Virtual API order is ABI** (§7).
* **The cache only covers the newest messages.** Anything else goes to PostgreSQL, so enabling
  it does not help historical or time-range queries. Use `shadowcompare` to verify a new
  deployment before letting it answer queries.
