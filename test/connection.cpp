#define BOOST_TEST_MODULE "MessageTypeClassModule"

#include "Connection.h"
#include "Config.h"

#include <boost/test/included/unit_test.hpp>
#include <typeinfo>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
BOOST_AUTO_TEST_CASE(connection_constructor_not_valid_args)
{
  BOOST_CHECK_THROW(
      { Connection connection("host", 123456789, "user", "passwd", "db", "encoding"); },
      Spine::Exception);
}

BOOST_AUTO_TEST_CASE(connection_constructor_valid_args,
                     *boost::unit_test::depends_on("connection_constructor_not_valid_args"))
{
  const bool boolVariable = true;
  const std::string filename = "cnf/valid.conf";
  Config conf(filename);
  Connection connection(conf.getHost(),
                        conf.getPort(),
                        conf.getUsername(),
                        conf.getPassword(),
                        conf.getDatabase(),
                        conf.getEncoding());
  BOOST_CHECK(typeid(connection.isConnected()) == typeid(boolVariable));
  BOOST_CHECK(connection.isConnected());
}

BOOST_AUTO_TEST_CASE(connection_accessors,
                     *boost::unit_test::depends_on("connection_constructor_valid_args"))
{
  const bool boolVariable = true;
  const std::string stringVariable;
  const std::string filename = "cnf/valid.conf";
  Config conf(filename);
  Connection connection(conf.getHost(),
                        conf.getPort(),
                        conf.getUsername(),
                        conf.getPassword(),
                        conf.getDatabase(),
                        conf.getEncoding());
  BOOST_CHECK(typeid(connection.collateSupported()) == typeid(boolVariable));
  BOOST_CHECK(not connection.collateSupported());

  BOOST_CHECK(typeid(connection.quote(filename)) == typeid(stringVariable));
  BOOST_CHECK_EQUAL(connection.quote(filename), std::string("'").append(filename).append("'"));
}

BOOST_AUTO_TEST_CASE(connection_query_select_age,
                     *boost::unit_test::depends_on("connection_constructor_valid_args"))
{
  const std::string filename = "cnf/valid.conf";
  Config conf(filename);
  Connection connection(conf.getHost(),
                        conf.getPort(),
                        conf.getUsername(),
                        conf.getPassword(),
                        conf.getDatabase(),
                        conf.getEncoding());
  connection.setDebug(true);

  const std::string selectAge("SELECT AGE(timestamp '2001-04-10', timestamp '1957-06-13');");
  pqxx::result selectAgeResult = connection.executeNonTransaction(selectAge);
  BOOST_CHECK_EQUAL(selectAgeResult.query(), selectAge);
  BOOST_CHECK(selectAgeResult.size() > 0);

  auto resultTuple = selectAgeResult.at(0);
  BOOST_CHECK_EQUAL(resultTuple["age"].c_str(), "43 years 9 mons 27 days");
}

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
