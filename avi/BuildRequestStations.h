#pragma once

#include "LocationOptions.h"

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
namespace BuildRequestStations
{
/*!
 * \brief Build 'request_stations' table (WITH clause) for request station id's
 * 		  (and position/order) for message query
 */
std::string withClause(const StationIdList& stationIdList, bool routeQuery);
}  // namespace BuildRequestStations
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
