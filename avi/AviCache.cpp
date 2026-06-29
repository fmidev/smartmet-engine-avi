// ======================================================================

#include "AviCache.h"
#include "Config.h"
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <duckdb.h>
#include <iostream>
#include <map>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
namespace
{
// Mirrored avidb_messages columns, in the order used by the schema and the
// appender. Kept here so CREATE TABLE and the loader stay in sync.
const char* kMessageCreate =
    "CREATE TABLE avidb_messages ("
    "station_id INTEGER, message_id BIGINT, message VARCHAR, "
    "message_time TIMESTAMP, valid_from TIMESTAMP, valid_to TIMESTAMP, "
    "created TIMESTAMP, file_modified TIMESTAMP, "
    "messir_heading VARCHAR, version VARCHAR, "
    "type_id INTEGER, format_id INTEGER, route_id INTEGER)";

const char* kMessageSelect =
    "SELECT station_id, message_id, message, message_time, valid_from, valid_to, "
    "created, file_modified, messir_heading, version, type_id, format_id, route_id "
    "FROM avidb_messages";

const char* kStationCreate =
    "CREATE TABLE avidb_stations ("
    "station_id INTEGER, icao_code VARCHAR, name VARCHAR, elevation INTEGER, "
    "valid_from TIMESTAMP, valid_to TIMESTAMP, modified_last TIMESTAMP, country_code VARCHAR)";

const char* kStationSelect =
    "SELECT station_id, icao_code, name, elevation, valid_from, valid_to, "
    "modified_last, country_code FROM avidb_stations";

const char* kTypeCreate =
    "CREATE TABLE avidb_message_types "
    "(type_id INTEGER, type VARCHAR, description VARCHAR, modified_last TIMESTAMP)";
const char* kTypeSelect =
    "SELECT type_id, type, description, modified_last FROM avidb_message_types";

const char* kFormatCreate = "CREATE TABLE avidb_message_format (format_id INTEGER, name VARCHAR)";
const char* kFormatSelect = "SELECT format_id, name FROM avidb_message_format";

const char* kRouteCreate =
    "CREATE TABLE avidb_message_routes "
    "(route_id INTEGER, name VARCHAR, description VARCHAR, modified_last TIMESTAMP)";
const char* kRouteSelect =
    "SELECT route_id, name, description, modified_last FROM avidb_message_routes";

// Column type tags for the generic loader
enum class ColType
{
  Int32,
  Int64,
  Str,
  Ts
};

// ----------------------------------------------------------------------
/*!
 * \brief IResult backend over a DuckDB result.
 *
 * Owns the result and the connection it was produced on; both are released on
 * destruction. Column lookup by name is resolved to an index once at construction.
 */
// ----------------------------------------------------------------------

class DuckResult : public IResult
{
 public:
  DuckResult(duckdb_connection con, duckdb_result result) : itsCon(con), itsResult(result)
  {
    idx_t n = duckdb_column_count(&itsResult);
    for (idx_t c = 0; c < n; c++)
      itsIndex.emplace(duckdb_column_name(&itsResult, c), c);
  }
  ~DuckResult() override
  {
    duckdb_destroy_result(&itsResult);
    duckdb_disconnect(&itsCon);
  }
  DuckResult(const DuckResult&) = delete;
  DuckResult& operator=(const DuckResult&) = delete;
  DuckResult(DuckResult&&) = delete;
  DuckResult& operator=(DuckResult&&) = delete;

  std::size_t size() const override { return duckdb_row_count(&itsResult); }

  bool isNull(std::size_t row, const std::string& column) const override
  {
    return duckdb_value_is_null(&itsResult, col(column), row);
  }
  int getInt(std::size_t row, const std::string& column) const override
  {
    return static_cast<int>(duckdb_value_int64(&itsResult, col(column), row));
  }
  long getLong(std::size_t row, const std::string& column) const override
  {
    return static_cast<long>(duckdb_value_int64(&itsResult, col(column), row));
  }
  double getDouble(std::size_t row, const std::string& column) const override
  {
    return duckdb_value_double(&itsResult, col(column), row);
  }
  std::string getString(std::size_t row, const std::string& column) const override
  {
    char* v = duckdb_value_varchar(&itsResult, col(column), row);
    std::string s(v ? v : "");
    if (v)
      duckdb_free(v);
    return s;
  }

 private:
  idx_t col(const std::string& name) const
  {
    auto it = itsIndex.find(name);
    if (it == itsIndex.end())
      throw Fmi::Exception(BCP, "AviCache: result has no column '" + name + "'");
    return it->second;
  }

  duckdb_connection itsCon;
  mutable duckdb_result itsResult;
  std::map<std::string, idx_t> itsIndex;
};

// Convert a PostgreSQL timestamp text (as parsed by the engine elsewhere) to
// DuckDB microseconds since the epoch.
duckdb_timestamp tsMicros(const std::string& s)
{
  duckdb_timestamp ts;
  ts.micros = (Fmi::DateTime::from_string(s) - Fmi::DateTime::epoch).total_microseconds();
  return ts;
}

void check(duckdb_state state, const char* what)
{
  if (state == DuckDBError)
    throw Fmi::Exception(BCP, std::string("AviCache: ") + what + " failed");
}

// Run a statement with no result on the given connection.
void exec(duckdb_connection con, const std::string& sql)
{
  duckdb_result r;
  if (duckdb_query(con, sql.c_str(), &r) == DuckDBError)
  {
    std::string err = duckdb_result_error(&r);
    duckdb_destroy_result(&r);
    throw Fmi::Exception(BCP, "AviCache statement failed: " + err).addParameter("SQL", sql);
  }
  duckdb_destroy_result(&r);
}

// Append one PostgreSQL column value (or NULL) to the appender per its type tag.
void appendField(duckdb_appender app, const pqxx::row& row, int field, ColType type)
{
  if (row[field].is_null())
  {
    check(duckdb_append_null(app), "append_null");
    return;
  }
  switch (type)
  {
    case ColType::Int32:
      check(duckdb_append_int32(app, row[field].as<int>()), "append_int32");
      break;
    case ColType::Int64:
      check(duckdb_append_int64(app, row[field].as<long>()), "append_int64");
      break;
    case ColType::Str:
      check(duckdb_append_varchar(app, row[field].as<std::string>().c_str()), "append_varchar");
      break;
    case ColType::Ts:
      check(duckdb_append_timestamp(app, tsMicros(row[field].as<std::string>())),
            "append_timestamp");
      break;
  }
}

}  // namespace

// ----------------------------------------------------------------------

AviCache::AviCache(const Config& config, Fmi::Database::PostgreSQLConnectionPool& pool)
    : itsConfig(config), itsPool(pool)
{
}

AviCache::~AviCache()
{
  shutdown();
}

void AviCache::start()
{
  itsThread = std::thread([this] { run(); });
}

void AviCache::shutdown()
{
  {
    std::lock_guard<std::mutex> lock(itsWakeupMutex);
    itsStop.store(true, std::memory_order_release);
  }
  itsWakeup.notify_all();

  if (itsThread.joinable())
    itsThread.join();

  if (itsDb != nullptr)
  {
    auto db = static_cast<duckdb_database>(itsDb);
    duckdb_close(&db);
    itsDb = nullptr;
  }
}

AviCache::Span AviCache::cachedSpan() const
{
  std::lock_guard<std::mutex> lock(itsSpanMutex);
  return itsSpan;
}

// ----------------------------------------------------------------------
/*!
 * \brief Background thread: build + initial load, then refresh on interval.
 */
// ----------------------------------------------------------------------

void AviCache::run()
{
  try
  {
    duckdb_database db = nullptr;
    if (duckdb_open(nullptr, &db) == DuckDBError)
      throw Fmi::Exception(BCP, "AviCache: failed to open in-memory DuckDB");
    itsDb = db;

    createSchema();
    fullLoad();
    refreshSpan();
    itsReady.store(true, std::memory_order_release);
    std::cout << "  -- AVI message cache initialized (DuckDB mirror ready)\n";
  }
  catch (...)
  {
    // Initial load failed: leave itsReady false so the engine keeps using
    // PostgreSQL. Log and still enter the loop to retry.
    Fmi::Exception ex(BCP, "AVI cache initial load failed; using PostgreSQL only");
    ex.printError();
  }

  const auto interval = std::chrono::seconds(itsConfig.getCacheUpdateIntervalSec());

  while (true)
  {
    {
      std::unique_lock<std::mutex> lock(itsWakeupMutex);
      itsWakeup.wait_for(
          lock, interval, [this] { return itsStop.load(std::memory_order_acquire); });
      if (itsStop.load(std::memory_order_acquire))
        break;
    }

    try
    {
      if (!itsReady.load(std::memory_order_acquire))
      {
        // A previous attempt failed; retry the full load.
        if (itsDb == nullptr)
        {
          duckdb_database db = nullptr;
          if (duckdb_open(nullptr, &db) == DuckDBError)
            continue;
          itsDb = db;
        }
        createSchema();
        fullLoad();
        refreshSpan();
        itsReady.store(true, std::memory_order_release);
        std::cout << "  -- AVI message cache initialized (retry succeeded)\n";
      }
      else
      {
        incrementalUpdate();
        refreshSpan();
      }
    }
    catch (...)
    {
      Fmi::Exception ex(BCP, "AVI cache refresh failed");
      ex.printError();
    }
  }
}

// ----------------------------------------------------------------------

void AviCache::createSchema()
{
  auto db = static_cast<duckdb_database>(itsDb);
  duckdb_connection con = nullptr;
  if (duckdb_connect(db, &con) == DuckDBError)
    throw Fmi::Exception(BCP, "AviCache: connect failed during schema creation");

  try
  {
    // All timestamps are treated as UTC, matching the engine's UTC handling.
    exec(con, "SET GLOBAL TimeZone='UTC'");

    // Drop any earlier tables (retry path) before recreating.
    for (const char* t : {"avidb_messages",
                          "avidb_stations",
                          "avidb_message_types",
                          "avidb_message_format",
                          "avidb_message_routes"})
      exec(con, std::string("DROP TABLE IF EXISTS ") + t);

    exec(con, kMessageCreate);
    exec(con, "CREATE INDEX ix_msg_time ON avidb_messages(message_time)");
    exec(con, "CREATE INDEX ix_msg_station ON avidb_messages(station_id)");
    exec(con, kStationCreate);
    exec(con, "CREATE INDEX ix_sta_id ON avidb_stations(station_id)");
    exec(con, kTypeCreate);
    exec(con, kFormatCreate);
    exec(con, kRouteCreate);
  }
  catch (...)
  {
    duckdb_disconnect(&con);
    throw;
  }
  duckdb_disconnect(&con);
}

// ----------------------------------------------------------------------
/*!
 * \brief Copy the rows of a PostgreSQL query into a mirror table via the appender.
 */
// ----------------------------------------------------------------------

namespace
{
void loadTable(duckdb_connection con,
               Fmi::Database::PostgreSQLConnectionPool& pool,
               const std::string& pgQuery,
               const char* table,
               const std::vector<ColType>& colTypes)
{
  auto connectionPtr = pool.get();
  auto& connection = *connectionPtr.get();
  pqxx::result result = connection.executeNonTransaction(pgQuery);

  duckdb_appender app = nullptr;
  if (duckdb_appender_create(con, nullptr, table, &app) == DuckDBError)
    throw Fmi::Exception(BCP, std::string("AviCache: appender_create failed for ") + table);

  try
  {
    for (const auto& row : result)
    {
      for (std::size_t c = 0; c < colTypes.size(); c++)
        appendField(app, row, static_cast<int>(c), colTypes[c]);
      check(duckdb_appender_end_row(app), "appender_end_row");
    }
    check(duckdb_appender_flush(app), "appender_flush");
  }
  catch (...)
  {
    duckdb_appender_destroy(&app);
    throw;
  }
  duckdb_appender_destroy(&app);
}

const std::vector<ColType> kMessageTypes = {ColType::Int32,
                                            ColType::Int64,
                                            ColType::Str,
                                            ColType::Ts,
                                            ColType::Ts,
                                            ColType::Ts,
                                            ColType::Ts,
                                            ColType::Ts,
                                            ColType::Str,
                                            ColType::Str,
                                            ColType::Int32,
                                            ColType::Int32,
                                            ColType::Int32};
const std::vector<ColType> kStationTypes = {ColType::Int32,
                                            ColType::Str,
                                            ColType::Str,
                                            ColType::Int32,
                                            ColType::Ts,
                                            ColType::Ts,
                                            ColType::Ts,
                                            ColType::Str};
const std::vector<ColType> kTypeTypes = {ColType::Int32, ColType::Str, ColType::Str, ColType::Ts};
const std::vector<ColType> kFormatTypes = {ColType::Int32, ColType::Str};
const std::vector<ColType> kRouteTypes = {ColType::Int32, ColType::Str, ColType::Str, ColType::Ts};
}  // namespace

void AviCache::fullLoad()
{
  auto db = static_cast<duckdb_database>(itsDb);
  duckdb_connection con = nullptr;
  if (duckdb_connect(db, &con) == DuckDBError)
    throw Fmi::Exception(BCP, "AviCache: connect failed during full load");

  try
  {
    const std::string window = " WHERE message_time >= (now() - INTERVAL '" +
                               Fmi::to_string(itsConfig.getCacheDurationHours()) + " hours')";

    loadTable(con, itsPool, std::string(kMessageSelect) + window, "avidb_messages", kMessageTypes);
    loadTable(con, itsPool, kStationSelect, "avidb_stations", kStationTypes);
    loadTable(con, itsPool, kTypeSelect, "avidb_message_types", kTypeTypes);
    loadTable(con, itsPool, kFormatSelect, "avidb_message_format", kFormatTypes);
    loadTable(con, itsPool, kRouteSelect, "avidb_message_routes", kRouteTypes);
  }
  catch (...)
  {
    duckdb_disconnect(&con);
    throw;
  }
  duckdb_disconnect(&con);
}

// ----------------------------------------------------------------------
/*!
 * \brief Refresh the mirror: upsert recent changes, reload lookups, expire.
 *
 * To stay robust against PostgreSQL/clock timezone differences we simply
 * re-fetch the last (updateInterval + safetyMargin) seconds of changes each
 * cycle and upsert them by message_id; the upsert is idempotent.
 */
// ----------------------------------------------------------------------

void AviCache::incrementalUpdate()
{
  auto db = static_cast<duckdb_database>(itsDb);
  duckdb_connection con = nullptr;
  if (duckdb_connect(db, &con) == DuckDBError)
    throw Fmi::Exception(BCP, "AviCache: connect failed during incremental update");

  try
  {
    const long lookback = static_cast<long>(itsConfig.getCacheUpdateIntervalSec()) +
                          itsConfig.getCacheSafetyMarginSec();
    const std::string recent = " (now() - INTERVAL '" + Fmi::to_string(lookback) + " seconds')";

    // 1) Upsert messages changed (inserted or re-parsed) within the lookback window.
    {
      const std::string pgQuery = std::string(kMessageSelect) + " WHERE created >= " + recent +
                                  " OR file_modified >= " + recent;

      auto connectionPtr = itsPool.get();
      auto& connection = *connectionPtr.get();
      pqxx::result result = connection.executeNonTransaction(pgQuery);

      if (!result.empty())
      {
        // Delete the affected ids first (idempotent upsert), then append.
        std::string ids;
        for (const auto& row : result)
        {
          if (!ids.empty())
            ids += ',';
          ids += Fmi::to_string(row["message_id"].as<long>());
        }
        exec(con, "DELETE FROM avidb_messages WHERE message_id IN (" + ids + ")");

        duckdb_appender app = nullptr;
        if (duckdb_appender_create(con, nullptr, "avidb_messages", &app) == DuckDBError)
          throw Fmi::Exception(BCP, "AviCache: appender_create failed for incremental messages");
        try
        {
          for (const auto& row : result)
          {
            for (std::size_t c = 0; c < kMessageTypes.size(); c++)
              appendField(app, row, static_cast<int>(c), kMessageTypes[c]);
            check(duckdb_appender_end_row(app), "appender_end_row");
          }
          check(duckdb_appender_flush(app), "appender_flush");
        }
        catch (...)
        {
          duckdb_appender_destroy(&app);
          throw;
        }
        duckdb_appender_destroy(&app);
      }
    }

    // 2) Reload the small lookup tables wholesale (cheap), and stations.
    exec(con, "DELETE FROM avidb_message_types");
    loadTable(con, itsPool, kTypeSelect, "avidb_message_types", kTypeTypes);
    exec(con, "DELETE FROM avidb_message_format");
    loadTable(con, itsPool, kFormatSelect, "avidb_message_format", kFormatTypes);
    exec(con, "DELETE FROM avidb_message_routes");
    loadTable(con, itsPool, kRouteSelect, "avidb_message_routes", kRouteTypes);

    // Upsert stations changed within the lookback window.
    {
      const std::string pgQuery = std::string(kStationSelect) + " WHERE modified_last >= " + recent;
      auto connectionPtr = itsPool.get();
      auto& connection = *connectionPtr.get();
      pqxx::result result = connection.executeNonTransaction(pgQuery);
      if (!result.empty())
      {
        std::string ids;
        for (const auto& row : result)
        {
          if (!ids.empty())
            ids += ',';
          ids += Fmi::to_string(row["station_id"].as<int>());
        }
        exec(con, "DELETE FROM avidb_stations WHERE station_id IN (" + ids + ")");

        duckdb_appender app = nullptr;
        if (duckdb_appender_create(con, nullptr, "avidb_stations", &app) == DuckDBError)
          throw Fmi::Exception(BCP, "AviCache: appender_create failed for incremental stations");
        try
        {
          for (const auto& row : result)
          {
            for (std::size_t c = 0; c < kStationTypes.size(); c++)
              appendField(app, row, static_cast<int>(c), kStationTypes[c]);
            check(duckdb_appender_end_row(app), "appender_end_row");
          }
          check(duckdb_appender_flush(app), "appender_flush");
        }
        catch (...)
        {
          duckdb_appender_destroy(&app);
          throw;
        }
        duckdb_appender_destroy(&app);
      }
    }

    // 3) Expire messages older than the configured window.
    exec(con,
         "DELETE FROM avidb_messages WHERE message_time < (current_timestamp - INTERVAL '" +
             Fmi::to_string(itsConfig.getCacheDurationHours()) + " hours')");
  }
  catch (...)
  {
    duckdb_disconnect(&con);
    throw;
  }
  duckdb_disconnect(&con);
}

// ----------------------------------------------------------------------

void AviCache::refreshSpan()
{
  Span span;
  try
  {
    auto result = executeEmbedded(
        "SELECT min(message_time)::VARCHAR AS lo, max(message_time)::VARCHAR AS hi "
        "FROM avidb_messages");
    if (result->size() == 1 && !result->isNull(0, "lo") && !result->isNull(0, "hi"))
    {
      span.start = Fmi::DateTime::from_string(result->getString(0, "lo"));
      span.end = Fmi::DateTime::from_string(result->getString(0, "hi"));
      span.valid = true;
    }
  }
  catch (...)
  {
    span.valid = false;
  }

  std::lock_guard<std::mutex> lock(itsSpanMutex);
  itsSpan = span;
}

// ----------------------------------------------------------------------

std::unique_ptr<IResult> AviCache::executeEmbedded(const std::string& sql) const
{
  auto db = static_cast<duckdb_database>(itsDb);
  if (db == nullptr)
    throw Fmi::Exception(BCP, "AviCache: database not open");

  duckdb_connection con = nullptr;
  if (duckdb_connect(db, &con) == DuckDBError)
    throw Fmi::Exception(BCP, "AviCache: connect failed");

  duckdb_result result;
  if (duckdb_query(con, sql.c_str(), &result) == DuckDBError)
  {
    std::string err = duckdb_result_error(&result);
    duckdb_destroy_result(&result);
    duckdb_disconnect(&con);
    throw Fmi::Exception(BCP, "AviCache query failed: " + err);
  }

  return std::make_unique<DuckResult>(con, result);
}

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet

// ======================================================================
