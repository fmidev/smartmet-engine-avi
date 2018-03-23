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

BOOST_AUTO_TEST_CASE(connection_constructor_valid_args)
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

//  BOOST_CHECK_EQUAL(messageTypes.size(), 1);
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
