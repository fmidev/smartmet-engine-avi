#pragma once

#include "Config.h"
#include "QueryOptions.h"
#include "Table.h"

#include <iostream>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
namespace BuildMessageQuery
{
/*!
 * \brief Build where clause for station id's for message query
 */
void whereStationIdInClause(const StationIdList& stationIdList, std::ostringstream& whereClause);

/*!
 * \brief Build from, where and order by clause with given station id's, message types,
 *		  tables and time instant/range for querying accepted messages
 */
void fromWhereOrderByClause(int maxMessageRows,
                            const StationIdList& stationIdList,
                            const QueryOptions& queryOptions,
                            const TableMap& tableMap,
                            const Config& config,
                            const Column* timeRangeColumn,
                            bool distinct,
                            std::ostringstream& fromWhereOrderByClause);
}  // namespace BuildMessageQuery
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
