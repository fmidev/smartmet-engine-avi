# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`smartmet-engine-avi` is a SmartMet Server engine that provides access to aviation weather data (METAR, TAF, SIGMET, and other message types) from a PostgreSQL/PostGIS database. It is loaded as a shared library (`avi.so`) by the SmartMet Server daemon and consumed by plugins (timeseries, edr, avi plugin, etc.).

## Build commands

```bash
make                # Build avi.so
make test           # Run all unit tests (requires a test database — see below)
make format         # Run clang-format on all source and test files
make clean          # Clean build artifacts
make rpm            # Build RPM package
```

### Running a single test

Tests compile to individual executables from `test/*.cpp`:

```bash
make -C test config.test && ./test/config.test --log_level=message
make -C test engine.test && ./test/engine.test --log_level=message
```

Each `*.cpp` in `test/` produces a corresponding `*.test` binary.

### Test database

Tests require a PostgreSQL test database. In CI, a local test database is created automatically. For local development, the test Makefile expects a `smartmet-test` host (or set `CI=1` to use a local temp database). The test config template `test/cnf/valid.conf.in` is processed via sed to produce `test/cnf/valid.conf` with the correct host.

## Architecture

### Class hierarchy

- **`Engine`** (`Engine.h`) — Public API base class inheriting `SmartMet::Spine::SmartMetEngine`. All public query methods are virtual with default implementations that throw "AVI engine not available". This allows plugins to link against the header without the engine actually being loaded.
- **`EngineImpl`** (`EngineImpl.h`) — Private implementation class that overrides all virtual methods. Manages a PostgreSQL connection pool and handles SQL query generation, validation, and result loading.
- **`Config`** (`Config.h`) — Parses libconfig configuration files. Defines `MessageType` objects that control time range queries, validity periods, message scoping (station/FIR/global), and Finnish METAR filtering rules.

### Query flow

1. Plugins call `queryStations()`, `queryMessages()`, or `queryStationsAndMessages()` with `QueryOptions`
2. `EngineImpl` validates inputs (station IDs, ICAO codes, countries, WKTs, time parameters, message types)
3. SQL is dynamically built from `Column`/`Table`/`QueryTable` structures — SELECT clauses, JOINs, and WHERE conditions are assembled based on the requested parameters
4. Queries use a CTE "record_set" pattern: first load candidate rows by `message_time` (indexed column) using configurable hour offsets, then apply message-type-specific time restrictions
5. Results are loaded into `StationQueryData` (keyed by station ID) or `QueryData` (for rejected messages)

### Key data types

- `LocationOptions` — Station selection by ID, ICAO code, bounding box, lon/lat, WKT geometry, place name, or country
- `TimeOptions` — Observation time, time ranges, format, timezone, and flags for controlling SIGMET/TAF-specific query behavior
- `QueryOptions` — Combines location, time, message type, format (TAC/IWXXM), and result-limiting options
- `StationQueryData` / `QueryData` — Result containers mapping station IDs to column value vectors

### Database schema assumptions

The engine queries against `avidb_stations`, `avidb_messages`, `avidb_rejected_messages`, `avidb_messages_routes`, `avidb_messages_types`, and `latest_messages` tables/views in a PostGIS-enabled PostgreSQL database.

## Configuration

Configuration uses libconfig format (`cnf/avi.conf.sample`). Key sections:

- `postgis` — Database connection (host, port, database, credentials, connection pool size)
- `message.types` — List of known message types with their time range type (`validtime`, `messagevalidtime`, `messagetime`, `creationtime`), validity hours, latest-message behavior, messir patterns, and query restriction settings
- `message.recordsetstarttimeoffsethours` / `recordsetendtimeoffsethours` — Expand the message_time CTE window to capture messages valid at the requested time (required, must be > 0)
- `message.filter_FI_METARxxx` — Finnish METAR deduplication filter

## Dependencies

- `smartmet-library-spine` (server framework, config base, HTTP)
- `smartmet-library-macgyver` (PostgreSQL connection pool, exceptions, time parsing, string utils)
- `smartmet-library-timeseries` (time series value types)
- `libpqxx` (PostgreSQL C++ client)
- `libconfig` (via `configpp` pkg-config)
