#pragma once

#include "LocationOptions.h"
#include "MessageType.h"
#include "TimeRangeType.h"

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
namespace BuildMessirHeading
{
/*!
 * \brief Build GROUP BY expression for messir_heading with given message types
 *		  (e.g. GAFOR) and time range type for querying latest accepted messages
 */
std::string groupByExpr(const StringList& messageTypeList,
                        const MessageTypes& knownMessageTypes,
                        TimeRangeType timeRangeType);
}  // namespace BuildMessirHeading
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
