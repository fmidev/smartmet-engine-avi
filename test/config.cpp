#define BOOST_TEST_MODULE "ConfigClassModule"

#include "Config.h"

#include <boost/test/included/unit_test.hpp>
#include <typeinfo>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
BOOST_AUTO_TEST_CASE(config_constructor_with_file_not_exist)
{
  const std::string filename = "cnf/notexist.conf";
  BOOST_CHECK_THROW({ Config config(filename); }, Spine::Exception);
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
  BOOST_CHECK_EQUAL(config.getHost(), "smartmet-test");

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
  BOOST_CHECK_EQUAL(config.getMessageTypes().size(), 10);
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
