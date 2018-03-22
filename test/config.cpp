#define BOOST_TEST_MODULE "ConfigClassModule"

#include "Config.h"

#include <boost/test/included/unit_test.hpp>
#include <memory>

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
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
