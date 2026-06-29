#define BOOST_TEST_MODULE "AviCacheModule"

#include "AviCache.h"
#include "Config.h"

#include <boost/test/included/unit_test.hpp>
#include <macgyver/PostgreSQLConnection.h>
#include <chrono>
#include <memory>
#include <thread>

// These tests exercise the DuckDB-backed in-memory cache against the test
// PostgreSQL database: they verify the mirror loads and that the embedded
// (DuckDB) engine returns the same results as PostgreSQL for identical SQL.
// They require a database (as the other engine tests do).

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
namespace
{
Fmi::Database::PostgreSQLConnectionOptions makeOptions(const Config& config)
{
  Fmi::Database::PostgreSQLConnectionOptions opt;
  opt.host = config.getHost();
  opt.port = static_cast<unsigned int>(config.getPort());
  opt.username = config.getUsername();
  opt.password = config.getPassword();
  opt.database = config.getDatabase();
  opt.encoding = config.getEncoding();
  return opt;
}

// Run a "SELECT ... AS n" returning a single long on PostgreSQL.
long pgScalar(Fmi::Database::PostgreSQLConnectionPool& pool, const std::string& sql)
{
  auto ptr = pool.get();
  auto& conn = *ptr.get();
  auto r = conn.executeNonTransaction(sql);
  return r[0]["n"].as<long>();
}

// Run a "SELECT ... AS n" returning a single long on the DuckDB mirror.
long cacheScalar(const AviCache& cache, const std::string& sql)
{
  auto r = cache.executeEmbedded(sql);
  return r->getLong(0, "n");
}

struct CacheFixture
{
  CacheFixture()
  {
    config = std::make_shared<Config>("cnf/valid.conf");
    pool = std::make_shared<Fmi::Database::PostgreSQLConnectionPool>(2, 4, makeOptions(*config));
    cache = std::make_shared<AviCache>(*config, *pool);
    cache->start();

    // Wait for the initial full load to complete.
    for (int i = 0; i < 300 && !cache->ready(); i++)
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  std::shared_ptr<Config> config;
  std::shared_ptr<Fmi::Database::PostgreSQLConnectionPool> pool;
  std::shared_ptr<AviCache> cache;
};

}  // namespace

BOOST_FIXTURE_TEST_SUITE(avicache, CacheFixture)

BOOST_AUTO_TEST_CASE(cache_becomes_ready)
{
  BOOST_REQUIRE(cache->ready());

  auto span = cache->cachedSpan();
  // The span is valid whenever there are any messages in the window. We only
  // require that querying it does not throw and that readiness was reached.
  BOOST_TEST_MESSAGE("cached span valid: " << span.valid);
}

BOOST_AUTO_TEST_CASE(mirror_message_count_matches_postgresql)
{
  BOOST_REQUIRE(cache->ready());

  const unsigned hours = config->getCacheDurationHours();
  const std::string window =
      "WHERE message_time >= (now() - INTERVAL '" + std::to_string(hours) + " hours')";

  long pg = pgScalar(*pool, "SELECT count(*) AS n FROM avidb_messages " + window);
  long mirror = cacheScalar(*cache, "SELECT count(*) AS n FROM avidb_messages");

  BOOST_CHECK_EQUAL(pg, mirror);
}

BOOST_AUTO_TEST_CASE(dialect_interval_and_extract_match_postgresql)
{
  BOOST_REQUIRE(cache->ready());

  // Exercise INTERVAL arithmetic, current_timestamp and EXTRACT on both engines
  // over a window safely inside the cached span.
  const std::string sql =
      "SELECT count(*) AS n FROM avidb_messages "
      "WHERE message_time >= (current_timestamp - INTERVAL '6 hours') "
      "AND EXTRACT(MINUTE FROM message_time) >= 0";

  long pg = pgScalar(*pool, sql);
  long mirror = cacheScalar(*cache, sql);

  BOOST_CHECK_EQUAL(pg, mirror);
}

BOOST_AUTO_TEST_CASE(dialect_date_trunc_grouping_matches_postgresql)
{
  BOOST_REQUIRE(cache->ready());

  // Number of distinct truncated hours within the last 12 hours.
  const std::string sql =
      "SELECT count(*) AS n FROM (SELECT DISTINCT DATE_TRUNC('hour', message_time) AS h "
      "FROM avidb_messages WHERE message_time >= (current_timestamp - INTERVAL '12 hours')) t";

  long pg = pgScalar(*pool, sql);
  long mirror = cacheScalar(*cache, sql);

  BOOST_CHECK_EQUAL(pg, mirror);
}

BOOST_AUTO_TEST_SUITE_END()

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
