#pragma once

#include "QueryOptions.h"

#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
namespace BuildRecordSet
{
/*!
 * \brief Build 'record_set' table (WITH clause) for querying accepted messages
 * 		 for given observation time or time range
 */
std::string withClause(bool routeQuery,
                       const StationIdList& stationIdList,
                       unsigned int startTimeOffsetHours,
                       unsigned int endTimeOffsetHours,
                       const std::string& obsOrRangeStartTime,
                       const std::string& rangeEndTimeOrEmpty = "");
}  // namespace BuildRecordSet
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
