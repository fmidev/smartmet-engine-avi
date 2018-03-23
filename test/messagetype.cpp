#define BOOST_TEST_MODULE "MessageTypeClassModule"

#include "Config.h"

#include <boost/test/included/unit_test.hpp>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
BOOST_AUTO_TEST_CASE(messagetype_addtype)
{
  const std::string messagetype1 = "METAR";
  MessageType obj;
  obj.addType(messagetype1);
  auto messageTypes = obj.getMessageTypes();
  BOOST_CHECK_EQUAL(messageTypes.size(), 1);
}

BOOST_AUTO_TEST_CASE(messagetype_getMessageTypes)
{
  const std::list<std::string> stringListVariable;
  const std::string messagetype1 = "METAR";
  MessageType obj;
  obj.addType(messagetype1);
  auto messageTypes = obj.getMessageTypes();
  BOOST_CHECK(typeid(messageTypes) == typeid(stringListVariable));
  BOOST_CHECK_EQUAL(messageTypes.size(), 1);
}

BOOST_AUTO_TEST_CASE(messagetype_equalto_operator)
{
  const std::string messagetype1 = "METAR";
  MessageType obj;
  BOOST_CHECK(not(obj == messagetype1));
  obj.addType(messagetype1);
  BOOST_CHECK(obj == messagetype1);
}

BOOST_AUTO_TEST_CASE(messagetype_hasValidityHours_default)
{
  const bool boolVariable = true;
  MessageType obj;
  BOOST_CHECK(typeid(obj.hasValidityHours()) == typeid(boolVariable));
  BOOST_CHECK(not obj.hasValidityHours());
}

BOOST_AUTO_TEST_CASE(messagetype_getTimeRangeType_default)
{
  const TimeRangeType timeRangeTypeVariable = TimeRangeType::NullTimeRange;
  MessageType obj;
  BOOST_CHECK(typeid(obj.getTimeRangeType()) == typeid(timeRangeTypeVariable));
  BOOST_CHECK_EQUAL(obj.getTimeRangeType(), TimeRangeType::NullTimeRange);
  BOOST_CHECK_EQUAL(obj.getTimeRangeType(), 0);
  BOOST_CHECK(not obj.hasValidityHours());
}

BOOST_AUTO_TEST_CASE(messagetype_setTimeRangeType_MessageTimeRange)
{
  MessageType obj;
  obj.setTimeRangeType(TimeRangeType::MessageTimeRange);
  BOOST_CHECK_EQUAL(obj.getTimeRangeType(), TimeRangeType::MessageTimeRange);
  BOOST_CHECK(obj.hasValidityHours());
}

BOOST_AUTO_TEST_CASE(messagetype_getValidityHours_default)
{
  const unsigned int unsignedIntVariable = 0;
  MessageType obj;
  BOOST_CHECK(typeid(obj.getValidityHours()) == typeid(unsignedIntVariable));
  BOOST_CHECK_EQUAL(obj.getValidityHours(), 0);
}

BOOST_AUTO_TEST_CASE(messagetype_setValidityHours_3)
{
  const unsigned int validityHours = 3;
  MessageType obj;
  obj.setValidityHours(validityHours);
  BOOST_CHECK_EQUAL(obj.getValidityHours(), validityHours);
}

BOOST_AUTO_TEST_CASE(messagetype_setLatestMessageOnly_default)
{
  const bool boolVariable = true;
  MessageType obj;
  BOOST_CHECK(typeid(obj.getLatestMessageOnly()) == typeid(boolVariable));
  BOOST_CHECK_EQUAL(obj.getLatestMessageOnly(), false);
}

BOOST_AUTO_TEST_CASE(messagetype_setLatestMessageOnly_true)
{
  MessageType obj;
  obj.setLatestMessageOnly(true);
  BOOST_CHECK_EQUAL(obj.getLatestMessageOnly(), true);
}

BOOST_AUTO_TEST_CASE(messagetype_getMessirPattern_default)
{
  const std::list<std::string> stringListVariable;
  MessageType obj;
  BOOST_CHECK(typeid(obj.getMessirPatterns()) == typeid(stringListVariable));
  BOOST_CHECK_EQUAL(obj.getMessirPatterns().size(), 0);
}

BOOST_AUTO_TEST_CASE(messagetype_addMessirPattern)
{
  const std::string messirPattern1("abc123");
  const std::string messirPattern2("aabbcc112233");
  MessageType obj;
  obj.addMessirPattern(messirPattern1);
  BOOST_CHECK_EQUAL(obj.getMessirPatterns().size(), 1);
  BOOST_CHECK_EQUAL(obj.getMessirPatterns().front(), messirPattern1);
  obj.addMessirPattern(messirPattern2);
  BOOST_CHECK_EQUAL(obj.getMessirPatterns().size(), 2);
  BOOST_CHECK_EQUAL(obj.getMessirPatterns().back(), messirPattern2);
}

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
