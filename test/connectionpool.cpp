#define BOOST_TEST_MODULE "ConnectionPoolModule"

#include "Config.h"
#include "ConnectionPool.h"

#include <boost/test/included/unit_test.hpp>
#include <chrono>
#include <string>
#include <thread>

// These tests exercise the idle connection recycling of ConnectionPool against the test
// PostgreSQL database: a connection reused within the max idle time keeps its server backend
// (same pg_backend_pid()), one idle longer is reopened (new backend). They require a database
// (as the other engine tests do).

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
namespace
{
Fmi::Database::PostgreSQLConnectionOptions makeOptions()
{
  const Config config("cnf/valid.conf");

  Fmi::Database::PostgreSQLConnectionOptions opt;
  opt.host = config.getHost();
  opt.port = static_cast<unsigned int>(config.getPort());
  opt.username = config.getUsername();
  opt.password = config.getPassword();
  opt.database = config.getDatabase();
  opt.encoding = config.getEncoding();
  return opt;
}

int backendPid(const ConnectionPool::Ptr& connection)
{
  return connection->executeNonTransaction("SELECT pg_backend_pid() AS pid")[0]["pid"].as<int>();
}

// Seconds since the connection's server backend was started
double backendAge(const ConnectionPool::Ptr& connection)
{
  return connection
      ->executeNonTransaction(
          "SELECT extract(epoch FROM now() - backend_start) AS age FROM pg_stat_activity "
          "WHERE pid = pg_backend_pid()")[0]["age"]
      .as<double>();
}

const std::chrono::seconds maxIdleTime(1);
const std::chrono::milliseconds idleLongerThanMax(1500);

}  // namespace

BOOST_AUTO_TEST_CASE(never_used_connection_is_reopened_when_idle_since_pool_creation)
{
  ConnectionPool pool(2, 2, makeOptions(), maxIdleTime);

  std::this_thread::sleep_for(idleLongerThanMax);

  // The start connections were opened with the pool; a backend younger than the idle period
  // means the connection was reopened on checkout
  auto connection = pool.get();
  BOOST_CHECK_LT(backendAge(connection), 1.0);
}

BOOST_AUTO_TEST_CASE(connection_reused_within_max_idle_time_keeps_its_backend)
{
  ConnectionPool pool(2, 2, makeOptions(), std::chrono::seconds(60));

  Fmi::Database::PostgreSQLConnection* first = nullptr;
  int pid = 0;
  {
    auto connection = pool.get();
    first = connection.get();
    pid = backendPid(connection);
  }

  auto connection = pool.get();
  BOOST_REQUIRE_EQUAL(connection.get(), first);
  BOOST_CHECK_EQUAL(backendPid(connection), pid);
}

BOOST_AUTO_TEST_CASE(connection_idle_longer_than_max_idle_time_is_reopened)
{
  ConnectionPool pool(2, 2, makeOptions(), maxIdleTime);

  Fmi::Database::PostgreSQLConnection* first = nullptr;
  int pid = 0;
  {
    auto connection = pool.get();
    first = connection.get();
    pid = backendPid(connection);
  }

  std::this_thread::sleep_for(idleLongerThanMax);

  int reopenedPid = 0;
  {
    auto connection = pool.get();
    BOOST_REQUIRE_EQUAL(connection.get(), first);
    reopenedPid = backendPid(connection);
    BOOST_CHECK_NE(reopenedPid, pid);
  }

  // Just reopened and released: kept as is
  auto connection = pool.get();
  BOOST_REQUIRE_EQUAL(connection.get(), first);
  BOOST_CHECK_EQUAL(backendPid(connection), reopenedPid);
}

BOOST_AUTO_TEST_CASE(zero_max_idle_time_never_recycles)
{
  ConnectionPool pool(2, 2, makeOptions());

  Fmi::Database::PostgreSQLConnection* first = nullptr;
  int pid = 0;
  {
    auto connection = pool.get();
    first = connection.get();
    pid = backendPid(connection);
  }

  std::this_thread::sleep_for(idleLongerThanMax);

  auto connection = pool.get();
  BOOST_REQUIRE_EQUAL(connection.get(), first);
  BOOST_CHECK_EQUAL(backendPid(connection), pid);
}

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
