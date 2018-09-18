#pragma once

#include "LocationOptions.h"

#include <sstream>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
namespace BuildStationQuery
{
/*!
 * \brief Build from and where clause with given coordinates and max distance
 *		  for querying stations
 */
void fromWhereClause(const LocationOptions& locationOptions,
                     const StringList& messageTypes,
                     std::ostringstream& fromWhereClause);

/*!
 * \brief Build where clause with given station id's for querying stations
 */
void whereClause(const StationIdList& stationIdList, std::ostringstream& whereClause);

/*!
 * \brief Build where clause with given icao codes or places (station names) for querying stations
 */
void whereClause(const std::string& columnExpression,
                 bool quoteLiteral,
                 const StringList& stringList,
                 std::ostringstream& whereClause);

/*!
 * \brief Build from and where (and order by for route query) clause with given wkts for querying
 * stations
 */
void fromWhereOrderByClause(const LocationOptions& locationOptions,
                            std::ostringstream& fromWhereOrderByClause);

/*!
 * \brief Build where clause with given bboxes for querying stations
 */
void whereClause(const BBoxList& bboxList, double maxDistance, std::ostringstream& whereClause);
}  // namespace BuildStationQuery
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
