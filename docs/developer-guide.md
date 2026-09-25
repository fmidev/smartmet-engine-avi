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
6. [Compatibility](#6-compatibility)
7. [Known pitfalls](#7-known-pitfalls)

---

## 1. Building and testing

```bash
make
make test                                  # Boost.Test programs in test/, need the test database
make -C test engine.test && ./test/engine.test --log_level=message
```

The tests use a PostgreSQL test database: the `smartmet-test` host, or with `CI=1` a
local temporary database. `test/cnf/valid.conf.in` is turned into `valid.conf` with the
right host.

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
4. loads the result into the result containers.

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

## 6. Compatibility

The public methods are virtual and plugins call them through the vtable: add new methods
**at the end** of `Engine` only. `QueryOptions`, `LocationOptions`, `TimeOptions` and the
result containers are built or read by the plugins (avi, timeseries, edr), so changing their
layout requires rebuilding those together with the engine.

## 7. Known pitfalls

* **A too narrow record-set window drops messages.** A message whose `message_time` is
  outside the CTE window is never considered, however long it is valid (a long-valid TAF
  or SIGMET). Keep the offsets at least as long as the longest validity.
* **Validation hits the database.** Station ids, ICAO codes, places, countries and WKTs are
  checked with database queries before the actual query runs.
* **Short ICAO strings are prefix matches.** `EF` means every station starting with `EF`.
* **Virtual API order is ABI** (§6).
