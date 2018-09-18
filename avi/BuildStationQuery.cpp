#include "BuildStationQuery.h"

#include "ConstantValues.h"

#include <spine/Exception.h>
#include <algorithm>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
void BuildStationQuery::fromWhereClause(const LocationOptions& locationOptions,
                                        const StringList& messageTypes,
                                        std::ostringstream& fromWhereClause)
{
  try
  {
    if (locationOptions.itsLonLats.empty())
      return;

    size_t n = 0;

    fromWhereClause << "FROM avidb_stations,(VALUES" << std::fixed << std::setprecision(10);

    for (auto const& lonlat : locationOptions.itsLonLats)
    {
      fromWhereClause << ((n == 0) ? " " : ")),") << "(ST_Point(" << lonlat.itsLon << ","
                      << lonlat.itsLat;
      n++;
    }

    fromWhereClause << "))) AS coordinates (" << stationCoordinateColumn
                    << ") WHERE ST_DWithin(geom::geography,ST_SetSRID(coordinates."
                    << stationCoordinateColumn << ",4326)::geography," << std::setprecision(0)
                    << locationOptions.itsMaxDistance << ")";

    // Note: When querying for given max # of nearest stations, stations having certain icaos codes
    // (starting with 'IL') are
    //		 ignored if messages of the only message type (AWSMETAR) originating from them will
    // be
    // ignored when querying messages
    //		 (when messages types were given but AWSMETAR was not included).
    //
    //		 This is to exclude nonapplicable stations when querying for TAFs, METARs etc

    const char* messageTypeSpecialToNearestStationSearch = "AWSMETAR";
    const char* stationIcaosSpecialToNearestStationSearch = "UPPER(icao_code) NOT LIKE 'IL%'";

    if ((locationOptions.itsNumberOfNearestStations > 0) && (!messageTypes.empty()) &&
        (std::find(messageTypes.begin(),
                   messageTypes.end(),
                   messageTypeSpecialToNearestStationSearch) == messageTypes.end()))
      fromWhereClause << " AND (" << stationIcaosSpecialToNearestStationSearch << ")";
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void BuildStationQuery::whereClause(const StationIdList& stationIdList,
                                    std::ostringstream& whereClause)
{
  try
  {
    if (stationIdList.empty())
      return;

    whereClause << (whereClause.str().empty() ? "WHERE (" : " OR (");

    size_t n = 0;

    for (auto const& stationId : stationIdList)
    {
      whereClause << ((n == 0) ? "station_id IN (" : ",") << stationId;
      n++;
    }

    whereClause << "))";
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void BuildStationQuery::whereClause(const std::string& columnExpression,
                                    bool quoteLiteral,
                                    const StringList& stringList,
                                    std::ostringstream& whereClause)
{
  try
  {
    if (stringList.empty())
      return;

    whereClause << (whereClause.str().empty() ? "WHERE (" : " OR (");

    size_t n = 0;

    for (auto const& str : stringList)
    {
      if (quoteLiteral)
        whereClause << ((n == 0) ? ("quote_literal(" + columnExpression + ") IN (") : ",")
                    << "UPPER(quote_literal('" << str << "'))";
      else
        whereClause << ((n == 0) ? (columnExpression + " IN (") : ",") << "UPPER('" << str << "')";

      n++;
    }

    whereClause << "))";
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void BuildStationQuery::fromWhereOrderByClause(const LocationOptions& locationOptions,
                                               std::ostringstream& fromWhereOrderByClause)
{
  try
  {
    if (locationOptions.itsWKTs.itsWKTs.empty())
      return;

    fromWhereOrderByClause << " FROM avidb_stations";

    if (locationOptions.itsWKTs.isRoute)
      // For route query the route segments (stations shortest distance to them) are used for
      // selecting the stations,
      // and segment indexes and starting points (stations distance to them) are used for ordering
      // the
      // stations along the route
      //
      // SELECT station_id[,...]
      // FROM avidb_stations,
      // (SELECT segindex,segstart,ST_SetSRID(ST_MakeLine(segstart,segend),4326) as segment
      //	FROM (SELECT ST_PointN(route,generate_series(1,ST_NPoints(route)-1)) as segstart,
      //		  ST_PointN(route,generate_series(2,ST_NPoints(route))) as segend,
      //		  generate_series(1,ST_NPoints(route)-1) as segindex
      //		  FROM (SELECT wkt::geometry as route) AS route
      //	     ) AS segpoints
      // ) AS segments
      //
      fromWhereOrderByClause
          << ",(SELECT segindex,segstart,ST_SetSRID(ST_MakeLine(segstart,segend),4326) as segment "
          << "FROM (SELECT ST_PointN(route,generate_series(1,ST_NPoints(route)-1)) as segstart,"
          << "ST_PointN(route,generate_series(2,ST_NPoints(route))) as segend,"
          << "generate_series(1,ST_NPoints(route)-1) as segindex "
          << "FROM (SELECT '" << locationOptions.itsWKTs.itsWKTs.front()
          << "'::geometry as route) AS route) AS segpoints) AS segments";

    fromWhereOrderByClause << " WHERE (";

    size_t n = 0;

    for (auto const& wkt : locationOptions.itsWKTs.itsWKTs)
    {
      std::ostringstream condition;

      if (locationOptions.itsWKTs.isRoute)
        // For a route (a single linestring), limit the stations by their shortest distance to route
        // segments
        //
        condition << "ST_DWithin(geom::geography,ST_ClosestPoint(segment,geom)::geography,"
                  << std::fixed << std::setprecision(0) << locationOptions.itsMaxDistance << ")";
      else
        // Limit by distance between geometries
        //
        condition << "ST_Length(ST_ShortestLine(geom,ST_GeogFromText('SRID=4326;" << wkt
                  << "')::geometry)::geography) <= " << std::fixed << std::setprecision(0)
                  << locationOptions.itsMaxDistance;

      fromWhereOrderByClause << ((n == 0) ? "(" : ") OR (") << condition.str();

      n++;
    }

    fromWhereOrderByClause << "))";

    // For a route (a single linestring), order the stations by route segment index and station's
    // distance to the start of the segment

    if (locationOptions.itsWKTs.isRoute)
      fromWhereOrderByClause
          << " ORDER BY segindex,ST_Distance(segstart::geography,geom::geography)";
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void BuildStationQuery::whereClause(const BBoxList& bboxList,
                                    double maxDistance,
                                    std::ostringstream& whereClause)
{
  try
  {
    if (bboxList.empty())
      return;

    whereClause << (whereClause.str().empty() ? "WHERE " : " OR ");

    size_t n = 0;

    for (auto const& bbox : bboxList)
    {
      std::ostringstream condition;

      condition << "(ST_Length(ST_ShortestLine(geom,ST_SetSRID(ST_MakeBox2D(ST_Point("
                << std::setprecision(10) << bbox.itsWest << "," << bbox.itsSouth << "),ST_Point("
                << bbox.itsEast << "," << bbox.itsNorth << ")),4326))::geography) <= " << std::fixed
                << std::setprecision(0) << maxDistance << ")";

      whereClause << ((n == 0) ? "" : " OR ") << condition.str();

      n++;
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
