#include "BuildRecordSet.h"

#include "BuildMessageQuery.h"
#include "ConstantValues.h"

#include <spine/Exception.h>
#include <iostream>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
std::string BuildRecordSet::withClause(bool routeQuery,
                                       const StationIdList& stationIdList,
                                       unsigned int startTimeOffsetHours,
                                       unsigned int endTimeOffsetHours,
                                       const std::string& obsOrRangeStartTime,
                                       const std::string& rangeEndTimeOrEmpty)
{
  try
  {
    /*
    record_set AS (
             SELECT *
             FROM avidb_messages me
             WHERE { me.station_id IN (stationIdList | SELECT station_id FROM request_stations) }
    AND
                       {
                         me.message_time >= observation time - INTERVAL 'n hours' AND
    me.message_time
    <= observation time + INTERVAL 'n hours' |
                         me.message_time >= starttime - INTERVAL 'n hours' AND me.message_time <=
    endtime + INTERVAL 'n hours'
                       }
    )
    */

    std::ostringstream withClause;
    std::string whereStationIdIn;

    if (!routeQuery)
      BuildMessageQuery::whereStationIdInClause(stationIdList, withClause);
    else
      withClause << " WHERE " << messageTableAlias << ".station_id IN (SELECT station_id FROM "
                 << requestStationsTable.itsName << ")";

    whereStationIdIn = withClause.str();
    withClause.str("");
    withClause.clear();

    withClause << recordSetTableName << " AS (SELECT * FROM " << messageTableName << " "
               << messageTableAlias << whereStationIdIn;

    const std::string& obsOrRangeEndTime =
        (rangeEndTimeOrEmpty.empty() ? obsOrRangeStartTime : rangeEndTimeOrEmpty);

    withClause << " AND " << messageTableAlias << ".message_time >= (" << obsOrRangeStartTime
               << " - INTERVAL '" << startTimeOffsetHours << " hours')"
               << " AND " << messageTableAlias << ".message_time <= (" << obsOrRangeEndTime
               << " + INTERVAL '" << endTimeOffsetHours << " hours')";

    withClause << ")";

    return withClause.str();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
