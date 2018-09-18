#pragma once

#include "LocationOptions.h"
#include "MessageType.h"

#include <list>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
namespace BuildMessageTimeRangeMessages
{
/*!
 * \brief Build 'messagetimerangelatest_messages' table (WITH clause) for
 *		  querying accepted messages having MessageTimeRangeLatest time
 *		  restriction with given time range
 */
std::string withClause(const StringList& messageTypes,
                       const MessageTypes& knownMessageTypes,
                       const std::string& startTime,
                       const std::string& endTime,
                       bool filterFIMETARxxx,
                       const std::list<std::string>& filterFIMETARxxxExcludeIcaos);
}  // namespace BuildMessageTimeRangeMessages
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
