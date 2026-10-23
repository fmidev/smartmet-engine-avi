#define BOOST_TEST_MODULE "ConfigClassModule"

#include "Config.h"

#include <boost/asio/ip/host_name.hpp>
#include <boost/test/included/unit_test.hpp>
#include <filesystem>
#include <fstream>
#include <system_error>
#include <typeinfo>
#include <unistd.h>
#include <utility>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
namespace
{
// Writes a test config with the given postgis settings and overrides into a temporary file and
// returns its name. The overrides are host specific, so they must be generated at run time.
std::string writeOverridesConfig(const std::string& thePostgisSettings,
                                 const std::string& theOverrides)
{
  const auto filename =
      (std::filesystem::temp_directory_path() / ("avi_config_overrides_" +
                                                 std::to_string(::getpid()) + ".conf"))
          .string();
  std::ofstream out(filename);
  out << "postgis:\n"
         "{\n"
         "  host = \"smartmet-test\";\n"
         "  port = 5444;\n"
         "  database = \"avi\";\n"
         "  username = \"avi_user\";\n"
         "  password = \"avi_pw\";\n"
         "  encoding = \"UTF8\";\n"
      << thePostgisSettings
      << "};\n"
         "message:\n"
         "{\n"
         "  maxstations = 0;\n"
         "  maxrows = 0;\n"
         "  recordsetstarttimeoffsethours = 30;\n"
         "  recordsetendtimeoffsethours = 12;\n"
         "  types = ( { name = \"METAR\"; timerangetype = \"messagetime\";"
         " validityhours = 2; latestmessage = true; } );\n"
         "};\n"
         "overrides:\n"
         "(\n"
      << theOverrides << ");\n";
  return filename;
}

// Removes the file when going out of scope, also when the test throws
struct TempFile
{
  explicit TempFile(std::string theName) : name(std::move(theName)) {}
  ~TempFile()
  {
    std::error_code ec;
    std::filesystem::remove(name, ec);
  }
  TempFile(const TempFile&) = delete;
  TempFile& operator=(const TempFile&) = delete;

  const std::string name;
};

const std::string allConnectionSettings =
    "    postgis:\n"
    "    {\n"
    "      connecttimeout = 10;\n"
    "      tcpusertimeout = 30000;\n"
    "      keepalivesidle = 60;\n"
    "      keepalivesinterval = 10;\n"
    "      keepalivescount = 3;\n"
    "      maxidletime = 300;\n"
    "    };\n";

}  // namespace

BOOST_AUTO_TEST_CASE(config_constructor_with_file_not_exist)
{
  const std::string filename = "cnf/notexist.conf";
  BOOST_CHECK_THROW({ Config config(filename); }, Fmi::Exception);
}
BOOST_AUTO_TEST_CASE(config_constructor_with_valid_file_exist)
{
  const std::string filename = "cnf/valid.conf";
  Config config(filename);
  BOOST_CHECK_EQUAL(config.get_file_name(), filename);
}
BOOST_AUTO_TEST_CASE(config_accessors,
                     *boost::unit_test::depends_on("config_constructor_with_valid_file_exist"))
{
  const std::string filename = "cnf/valid.conf";
  Config config(filename);
  const std::string stringVariable;
  const int intVariable = 0;
  const bool boolVariable = true;

  BOOST_CHECK(typeid(config.getHost()) == typeid(stringVariable));
  // BOOST_CHECK_EQUAL(config.getHost(), "smartmet-test");

  BOOST_CHECK(typeid(config.getPort()) == typeid(intVariable));
  BOOST_CHECK_EQUAL(config.getPort(), 5444);

  BOOST_CHECK(typeid(config.getDatabase()) == typeid(stringVariable));
  BOOST_CHECK_EQUAL(config.getDatabase(), "avi");

  BOOST_CHECK(typeid(config.getUsername()) == typeid(stringVariable));
  BOOST_CHECK_EQUAL(config.getUsername(), "avi_user");

  BOOST_CHECK(typeid(config.getPassword()) == typeid(stringVariable));
  BOOST_CHECK_EQUAL(config.getPassword(), "avi_pw");

  BOOST_CHECK(typeid(config.getEncoding()) == typeid(stringVariable));
  BOOST_CHECK_EQUAL(config.getEncoding(), "UTF8");

  BOOST_CHECK(typeid(config.getMaxMessageStations()) == typeid(intVariable));
  BOOST_CHECK_EQUAL(config.getMaxMessageStations(), 0);

  BOOST_CHECK(typeid(config.getMaxMessageRows()) == typeid(intVariable));
  BOOST_CHECK_EQUAL(config.getMaxMessageRows(), 0);

  BOOST_CHECK(typeid(config.getRecordSetStartTimeOffsetHours()) == typeid(intVariable));
  BOOST_CHECK_EQUAL(config.getRecordSetStartTimeOffsetHours(), 30);

  BOOST_CHECK(typeid(config.getRecordSetEndTimeOffsetHours()) == typeid(intVariable));
  BOOST_CHECK_EQUAL(config.getRecordSetEndTimeOffsetHours(), 12);

  BOOST_CHECK(typeid(config.getFilterFIMETARxxx()) == typeid(boolVariable));
  BOOST_CHECK_EQUAL(config.getFilterFIMETARxxx(), true);

  const std::list<std::string> filterFIMETARxxxExcludeIcaosEmpty;
  BOOST_CHECK(typeid(config.getFilterFIMETARxxxExcludeIcaos()) ==
              typeid(filterFIMETARxxxExcludeIcaosEmpty));
  BOOST_CHECK_EQUAL(config.getFilterFIMETARxxxExcludeIcaos().size(), 2);
  BOOST_CHECK_EQUAL(config.getFilterFIMETARxxxExcludeIcaos().front(), "EFHF");
  BOOST_CHECK_EQUAL(config.getFilterFIMETARxxxExcludeIcaos().back(), "EFUT");

  MessageTypes messageTypesEmpty;
  BOOST_CHECK(typeid(config.getMessageTypes()) == typeid(messageTypesEmpty));
  BOOST_CHECK_EQUAL(config.getMessageTypes().size(), 9);
}

BOOST_AUTO_TEST_CASE(config_cache_defaults,
                     *boost::unit_test::depends_on("config_constructor_with_valid_file_exist"))
{
  // valid.conf has no message.cache block: the cache must default to disabled.
  const std::string filename = "cnf/valid.conf";
  Config config(filename);

  BOOST_CHECK_EQUAL(config.getCacheEnabled(), false);
  BOOST_CHECK_EQUAL(config.getCacheDurationHours(), 48u);
  BOOST_CHECK_EQUAL(config.getCacheUpdateIntervalSec(), 10u);
  BOOST_CHECK_EQUAL(config.getCacheSafetyMarginSec(), 600u);
  BOOST_CHECK_EQUAL(config.getCacheShadowCompare(), false);
  BOOST_CHECK_EQUAL(config.getCacheMemoryLimitMb(), 0u);
}

BOOST_AUTO_TEST_CASE(config_cache_enabled_settings)
{
  const std::string filename = "cnf/cache_valid.conf";
  Config config(filename);

  BOOST_CHECK_EQUAL(config.getCacheEnabled(), true);
  BOOST_CHECK_EQUAL(config.getCacheDurationHours(), 48u);
  BOOST_CHECK_EQUAL(config.getCacheUpdateIntervalSec(), 15u);
  BOOST_CHECK_EQUAL(config.getCacheSafetyMarginSec(), 300u);
  BOOST_CHECK_EQUAL(config.getCacheShadowCompare(), true);
  BOOST_CHECK_EQUAL(config.getCacheMemoryLimitMb(), 512u);
}

BOOST_AUTO_TEST_CASE(config_connection_defaults,
                     *boost::unit_test::depends_on("config_constructor_with_valid_file_exist"))
{
  // valid.conf sets none of the dead connection detection settings: all must default to 0.
  const std::string filename = "cnf/valid.conf";
  Config config(filename);

  BOOST_CHECK_EQUAL(config.getConnectTimeout(), 0u);
  BOOST_CHECK_EQUAL(config.getTcpUserTimeout(), 0u);
  BOOST_CHECK_EQUAL(config.getKeepalivesIdle(), 0u);
  BOOST_CHECK_EQUAL(config.getKeepalivesInterval(), 0u);
  BOOST_CHECK_EQUAL(config.getKeepalivesCount(), 0u);
  BOOST_CHECK_EQUAL(config.getMaxIdleTime(), 0u);
}

BOOST_AUTO_TEST_CASE(config_connection_settings)
{
  const std::string filename = "cnf/connection_valid.conf";
  Config config(filename);

  BOOST_CHECK_EQUAL(config.getConnectTimeout(), 10u);
  BOOST_CHECK_EQUAL(config.getTcpUserTimeout(), 30000u);
  BOOST_CHECK_EQUAL(config.getKeepalivesIdle(), 60u);
  BOOST_CHECK_EQUAL(config.getKeepalivesInterval(), 10u);
  BOOST_CHECK_EQUAL(config.getKeepalivesCount(), 3u);
  BOOST_CHECK_EQUAL(config.getMaxIdleTime(), 300u);
}

BOOST_AUTO_TEST_CASE(config_connection_settings_overrides_matching_host)
{
  // A matching overrides entry takes precedence over the postgis section value (maxidletime 999)
  // and over a non-matching entry listed before it
  const TempFile file(writeOverridesConfig(
      "  maxidletime = 999;\n",
      "  {\n"
      "    name = [\"no-such-host-avi-test\"];\n"
      "    postgis: { connecttimeout = 1; tcpusertimeout = 1; maxidletime = 1; };\n"
      "  },\n"
      "  {\n"
      "    name = [\"no-such-host-avi-test-2\", \"" +
          boost::asio::ip::host_name() + "\"];\n" + allConnectionSettings + "  }\n"));
  Config config(file.name);

  BOOST_CHECK_EQUAL(config.getConnectTimeout(), 10u);
  BOOST_CHECK_EQUAL(config.getTcpUserTimeout(), 30000u);
  BOOST_CHECK_EQUAL(config.getKeepalivesIdle(), 60u);
  BOOST_CHECK_EQUAL(config.getKeepalivesInterval(), 10u);
  BOOST_CHECK_EQUAL(config.getKeepalivesCount(), 3u);
  BOOST_CHECK_EQUAL(config.getMaxIdleTime(), 300u);
}

BOOST_AUTO_TEST_CASE(config_connection_settings_overrides_other_hosts)
{
  // Settings in overrides entries for other hosts are not in effect: values not set in the
  // postgis section stay 0 (disabled), values set there apply
  const TempFile file(writeOverridesConfig("  connecttimeout = 5;\n",
                                           "  {\n"
                                           "    name = [\"no-such-host-avi-test\"];\n" +
                                               allConnectionSettings + "  }\n"));
  Config config(file.name);

  BOOST_CHECK_EQUAL(config.getConnectTimeout(), 5u);
  BOOST_CHECK_EQUAL(config.getTcpUserTimeout(), 0u);
  BOOST_CHECK_EQUAL(config.getKeepalivesIdle(), 0u);
  BOOST_CHECK_EQUAL(config.getKeepalivesInterval(), 0u);
  BOOST_CHECK_EQUAL(config.getKeepalivesCount(), 0u);
  BOOST_CHECK_EQUAL(config.getMaxIdleTime(), 0u);
}

BOOST_AUTO_TEST_CASE(config_cache_duration_invariant)
{
  // durationhours (20) must be greater than recordsetstarttimeoffsethours (30): reject.
  const std::string filename = "cnf/cache_invalid.conf";
  BOOST_CHECK_THROW({ Config config(filename); }, Fmi::Exception);
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
