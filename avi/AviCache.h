// ======================================================================
/*!
 * \brief In-memory cache of recent aviation messages backed by DuckDB
 *
 * The AVI engine generates PostgreSQL query text for selecting valid/latest
 * messages. Under load, "latest messages at current time" requests (EDR,
 * timeseries) all reach the PostgreSQL backend. AviCache keeps the last N
 * hours of the avidb_* tables in an in-process DuckDB database and runs that
 * very same generated SQL against the mirror, so cacheable requests avoid the
 * PostgreSQL backend entirely.
 *
 * A single background thread performs the initial full load and then refreshes
 * the mirror on a fixed interval (incremental upsert by created/file_modified
 * high-water mark + expiry of rows older than the configured window). Reads use
 * their own short-lived DuckDB connections; DuckDB MVCC gives them a consistent
 * snapshot while the updater commits.
 *
 * Until the first full load completes (itsReady) and for any request whose time
 * window is not fully inside the cached span, the engine falls back to the
 * PostgreSQL path, so the cache is always a pure optimization.
 */
// ======================================================================

#pragma once

#include "ResultAccessor.h"
#include <macgyver/DateTime.h>
#include <macgyver/PostgreSQLConnection.h>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
class Config;

class AviCache
{
 public:
  struct Span
  {
    bool valid = false;
    Fmi::DateTime start;
    Fmi::DateTime end;
  };

  AviCache(const Config& config, Fmi::Database::PostgreSQLConnectionPool& pool);
  ~AviCache();

  AviCache() = delete;
  AviCache(const AviCache&) = delete;
  AviCache& operator=(const AviCache&) = delete;
  AviCache(AviCache&&) = delete;
  AviCache& operator=(AviCache&&) = delete;

  // Launch the background loader/updater thread.
  void start();

  // Stop the background thread and release the DuckDB database.
  void shutdown();

  // True once the initial full load has completed and the mirror is usable.
  bool ready() const { return itsReady.load(std::memory_order_acquire); }

  // The [min(message_time), max(message_time)] span currently held in the mirror.
  Span cachedSpan() const;

  // Execute generated SQL against the mirror and return the result. Throws on
  // SQL/engine errors so the caller can fall back to PostgreSQL.
  std::unique_ptr<IResult> executeEmbedded(const std::string& sql) const;

 private:
  void run();                // background thread body
  void createSchema();       // DDL + GLOBAL TimeZone=UTC
  void fullLoad();           // initial population of all mirror tables
  void incrementalUpdate();  // fetch new/updated rows, upsert, expire, refresh span
  void refreshSpan();        // recompute cachedSpan from the mirror

  const Config& itsConfig;
  Fmi::Database::PostgreSQLConnectionPool& itsPool;

  // DuckDB database handle (duckdb_database). Stored as void* so the large
  // duckdb.h need not be included from this header; cast in AviCache.cpp.
  void* itsDb = nullptr;

  std::atomic<bool> itsReady{false};
  std::atomic<bool> itsStop{false};
  std::thread itsThread;
  std::condition_variable itsWakeup;
  std::mutex itsWakeupMutex;

  mutable std::mutex itsSpanMutex;
  Span itsSpan;
};  // class AviCache

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet

// ======================================================================
