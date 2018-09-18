#include "BuildRequestStations.h"

#include "ConstantValues.h"

#include <spine/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
std::string BuildRequestStations::withClause(const StationIdList& stationIdList, bool routeQuery)
{
  try
  {
    /*
    WITH request_stations AS (
            SELECT request_stations.station_id[,request_stations.position]
            FROM (VALUES ((),...)) AS request_stations (station_id[,position])
    )
    */

    std::ostringstream withClause;

    withClause << "WITH " << requestStationsTable.itsName
               << " AS (SELECT request_stations.station_id"
               << (routeQuery ? std::string(",request_stations.") + requestStationsPositionColumn
                              : "")
               << " FROM (VALUES ";

    size_t n = 0;

    for (auto stationId : stationIdList)
    {
      withClause << ((n == 0) ? "(" : "),(") << stationId;

      if (routeQuery)
        withClause << "," << n;

      n++;
    }

    withClause << ")) AS request_stations (station_id"
               << (routeQuery ? std::string(",") + requestStationsPositionColumn : "") << "))";

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
