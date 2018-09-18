#pragma once

#include "MessageType.h"
#include "QueryOptions.h"

#include <list>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
namespace BuildMessageType
{
/*!
 * \brief Build GROUP BY expression for message type with given message types
 *		  (e.g. METREP,SPECIAL) and time range type for querying latest accepted messages
 */
std::string groupByExpr(const StringList& messageTypeList,
                        const MessageTypes& knownMessageTypes,
                        TimeRangeType timeRangeType);

/*!
 * \brief Build message type IN clause with given message and time range type(s)
 */
std::string inClause(const StringList& messageTypeList,
                     const MessageTypes& knownMessageTypes,
                     std::list<TimeRangeType> timeRangeTypes);

std::string inClause(const StringList& messageTypeList,
                     const MessageTypes& knownMessageTypes,
                     TimeRangeType timeRangeType);

/*!
 * \brief Build 'message_validity' table (WITH clause) for requested message
 * 		  types and their configured validity in hours
 */
std::string validityWithClause(const StringList messageTypeList,
                               const MessageTypes& knownMessageTypes);
}  // namespace BuildMessageType
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
