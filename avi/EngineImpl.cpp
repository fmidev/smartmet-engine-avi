// ======================================================================

#include "EngineImpl.h"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <macgyver/AnsiEscapeCodes.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <macgyver/TimeParser.h>
#include <spine/Convenience.h>
#include <memory>
#include <stdexcept>

using namespace std;

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
namespace
{
Fmi::TimeZonePtr& tzUTC = Fmi::TimeZonePtr::utc;

Fmi::Database::PostgreSQLConnectionOptions mk_connection_options(Config& itsConfig)
{
  Fmi::Database::PostgreSQLConnectionOptions opt;
  opt.host = itsConfig.getHost();
  opt.port = itsConfig.getPort();
  opt.username = itsConfig.getUsername();
  opt.password = itsConfig.getPassword();
  opt.database = itsConfig.getDatabase();
  opt.encoding = itsConfig.getEncoding();
  return opt;
}

template <typename Type, typename... Parts>
Type value_or(const std::variant<Parts...>& arg, const Type& value)
{
  const Type* ptr = std::get_if<Type>(&arg);
  return ptr ? *ptr : value;
}

// ----------------------------------------------------------------------
/*!
 * \brief Return derived column select expression
 */
// ----------------------------------------------------------------------

string derivedExpression(const string& tableColumnName, const string& queryColumnName)
{
  // ST_X(geom) AS longitudes
  // ST_Y(geom) AS latitude
  // ST_X(geom) || ',' || ST_Y(geom) AS lonlat
  // ST_Y(geom) || ',' || ST_X(geom) AS latlon
  // ST_Distance(geom::geography,ST_SetSRID(coordinates.coordinate,4326)::geography) AS distance
  // DEGREES(ST_Azimuth(geom,ST_SetSRID(coordinates.coordinate,4326))) AS bearing

  return tableColumnName + " AS " + queryColumnName;
}

// ----------------------------------------------------------------------
/*!
 * \brief Return 'NULL' select expression
 */
// ----------------------------------------------------------------------

string nullExpression(const Column* queryColumn)
{
  // NULL AS distance
  // NULL AS bearing

  return string("NULL AS ") + queryColumn->itsName;
}

// Derived column expressions

const char* dfLongitude = "ST_X(geom)";
const char* dfLatitude = "ST_Y(geom)";
const char* dfLonLat = "('' || ST_X(geom) || ',' || ST_Y(geom))";
const char* dfLatLon = "('' || ST_Y(geom) || ',' || ST_X(geom))";
const char* dfDistance =
    "ST_Distance(geom::geography,ST_SetSRID(coordinates.coordinate,4326)::geography) / 1000";
const char* dfBearing = "DEGREES(ST_Azimuth(geom,ST_SetSRID(coordinates.coordinate,4326)))";

// Some table/column etc. names/joins

const char* firTableName = "icao_fir_yhdiste";
const char* firTableAlias = "fi";
const char* firTableJoin = "ST_Contains(fi.areageom,st.geom)";
const char* firIdTableColumn = "gid";
const char* firIdQueryColumn = "firid";
const char* stationTableName = "avidb_stations";
const char* stationTableAlias = "st";
const char* stationTableJoin = "st.station_id = me.station_id";
const char* stationIcaoTableColumn = "icao_code";
const char* stationIcaoQueryColumn = "icao";
const char* stationDistanceQueryColumn = "distance";
const char* stationBearingQueryColumn = "bearing";
const char* stationCoordinateColumn = "coordinate";
const char* stationLonLatQueryColumn = "lonlat";
const char* stationLatLonQueryColumn = "latlon";
const char* stationCountryCodeTableColumn = "country_code";
const char* messageIdTableColumn = "message_id";
const char* messageStationIdTableColumn = "station_id";
const char* messageTableColumn = messageQueryColumn;
const char* rejectedMessageIcaoTableColumn = "icao_code";
const char* recordSetTableName = "record_set";
const char* messageTableName = "avidb_messages";
const char* messageTableAlias = "me";
const char* messageTypeTableName = "avidb_message_types";
const char* messageTypeTableAlias = "mt";
const char* messageTypeTableJoin = "me.type_id = mt.type_id";
const char* messageFormatTableName = "avidb_message_format";
const char* messageFormatTableAlias = "mf";
const char* messageFormatTableJoinTAC = "me.format_id = mf.format_id AND mf.name = 'TAC'";
const char* messageFormatTableJoinIWXXM = "me.format_id = mf.format_id AND mf.name = 'IWXXM'";
const char* messageRouteTableName = "avidb_message_routes";
const char* messageRouteTableAlias = "mr";
const char* messageRouteTableJoin = "me.route_id = mr.route_id";
const char* rejectedMessageTableName = "avidb_rejected_messages";
const char* rejectedMessageTableAlias = messageTableAlias;
const char* rejectedMessageTypeTableJoin = messageTypeTableJoin;
const char* rejectedMessageRouteTableJoin = messageRouteTableJoin;
const char* latestMessagesTableJoin = "me.message_id IN (SELECT message_id FROM latest_messages)";
const char* requestStationsTableAlias = "rs";
const char* requestStationsPositionColumn = "position";
const char* requestStationsTableJoin = "rs.station_id = me.station_id";
const char* messageValidityTableName = "message_validity";
const char* messageValidityTableAlias = "mv";
const char* messageValidityTableJoin = "mv.type = mt.type";
const char* messageTimeRangeLatestMessagesTableName = "messagetimerangelatest_messages";

// Table/query column mapping

Column firQueryColumns[] = {
    //
    // Table columns
    //
    // Type, Table column, Query column
    //
    {ColumnType::Integer, firIdTableColumn, firIdQueryColumn},
    {ColumnType::None, "", "", nullptr, nullptr}};

Column stationQueryColumns[] = {
    //
    // Table columns
    //
    // Type, Table column, Query column
    //
    {ColumnType::Integer, "station_id", stationIdQueryColumn},
    {ColumnType::String, stationIcaoTableColumn, stationIcaoQueryColumn},
    {ColumnType::String, "name", "name"},
    {ColumnType::Integer, "elevation", "elevation"},
    {ColumnType::DateTime, "valid_from", "stationvalidfrom"},
    {ColumnType::DateTime, "valid_to", "stationvalidto"},
    {ColumnType::DateTime, "modified_last", "stationmodified"},
    {ColumnType::String, stationCountryCodeTableColumn, "iso2"},
    //
    // Derived columns
    //
    // Type, Derived column, Query column, f(Table column), f(Table column)
    //
    {ColumnType::Double, dfLongitude, "longitude", derivedExpression, nullptr},
    {ColumnType::Double, dfLatitude, "latitude", derivedExpression, nullptr},
    {ColumnType::TS_LonLat, dfLonLat, stationLonLatQueryColumn, derivedExpression, nullptr},
    {ColumnType::TS_LatLon, dfLatLon, stationLatLonQueryColumn, derivedExpression, nullptr},
    {ColumnType::Double, dfDistance, stationDistanceQueryColumn, nullptr, derivedExpression},
    {ColumnType::Double, dfBearing, stationBearingQueryColumn, nullptr, derivedExpression},
    {ColumnType::None, "", "", nullptr, nullptr}};

Column messageTypeQueryColumns[] = {
    //
    // Table columns
    //
    // Type, Table column, Query column
    //
    {ColumnType::String, "type", "messagetype"},
    {ColumnType::String, "description", "messagetypedescription"},
    {ColumnType::DateTime, "modified_last", "messagetypemodified"},
    {ColumnType::None, "", ""}};

Column messageRouteQueryColumns[] = {
    //
    // Table columns
    //
    // Type, Table column, Query column
    //
    {ColumnType::String, "name", "route"},
    {ColumnType::String, "description", "routedescription"},
    {ColumnType::DateTime, "modified_last", "routemodified"},
    {ColumnType::None, "", ""}};

Column messageQueryColumns[] = {
    //
    // Table columns
    //
    // Type, Table column, Query column
    //
    {ColumnType::Integer, "station_id", stationIdQueryColumn},
    {ColumnType::Integer, messageIdTableColumn, "messageid"},
    {ColumnType::String, messageTableColumn, messageQueryColumn},
    {ColumnType::DateTime, "message_time", "messagetime"},
    {ColumnType::DateTime, "valid_from", "messagevalidfrom"},
    {ColumnType::DateTime, "valid_to", "messagevalidto"},
    {ColumnType::DateTime, "created", "messagecreated"},
    {ColumnType::DateTime, "file_modified", "messagefilemodified"},
    {ColumnType::String, "messir_heading", "messirheading"},
    {ColumnType::String, "version", "messageversion"},
    {ColumnType::None, "", ""}};

Column rejectedMessageQueryColumns[] = {
    //
    // Table columns
    //
    // Type, Table column, Query column
    //
    {ColumnType::String, "icao_code", "messagerejectedicao"},
    {ColumnType::String, "message", "message"},
    {ColumnType::DateTime, "message_time", "messagetime"},
    {ColumnType::DateTime, "valid_from", "messagevalidfrom"},
    {ColumnType::DateTime, "valid_to", "messagevalidto"},
    {ColumnType::DateTime, "created", "messagecreated"},
    {ColumnType::DateTime, "file_modified", "messagefilemodified"},
    {ColumnType::String, "messir_heading", "messirheading"},
    {ColumnType::String, "version", "messageversion"},
    {ColumnType::Integer, "reject_reason", "messagerejectedreason"},
    {ColumnType::None, "", ""}};

QueryTable firQueryTables[] = {{firTableName, firTableAlias, firQueryColumns, firTableJoin},
                               {"", "", nullptr, ""}};

QueryTable messageQueryTables[] = {
    {messageTableName, messageTableAlias, messageQueryColumns, ""},
    {messageTypeTableName, messageTypeTableAlias, messageTypeQueryColumns, messageTypeTableJoin},
    {messageRouteTableName,
     messageRouteTableAlias,
     messageRouteQueryColumns,
     messageRouteTableJoin},
    {stationTableName, stationTableAlias, stationQueryColumns, stationTableJoin},
    {"", "", nullptr, ""}};

Column noQueryColumns[] = {{ColumnType::None, "", ""}};

QueryTable requestStationsTable = {
    "request_stations", requestStationsTableAlias, noQueryColumns, requestStationsTableJoin};
QueryTable messageValidityTable = {
    "message_validity", messageValidityTableAlias, noQueryColumns, messageValidityTableJoin};
QueryTable latestMessagesTable = {"latest_messages", "lm", noQueryColumns, latestMessagesTableJoin};

QueryTable rejectedMessageQueryTables[] = {
    {rejectedMessageTableName, rejectedMessageTableAlias, rejectedMessageQueryColumns, ""},
    {messageTypeTableName,
     messageTypeTableAlias,
     messageTypeQueryColumns,
     rejectedMessageTypeTableJoin},
    {messageRouteTableName,
     messageRouteTableAlias,
     messageRouteQueryColumns,
     rejectedMessageRouteTableJoin},
    {"", "", nullptr, ""}};

// ----------------------------------------------------------------------
/*!
 * \brief Build from and where clause with given coordinates and max distance
 *        for querying stations
 */
// ----------------------------------------------------------------------

void buildStationQueryFromWhereClause(const LocationOptions& locationOptions,
                                      const StringList& messageTypes,
                                      ostringstream& fromWhereClause)
{
  try
  {
    if (locationOptions.itsLonLats.empty())
      return;

    size_t n = 0;

    fromWhereClause << "FROM avidb_stations,(VALUES" << fixed << setprecision(10);

    for (auto const& lonlat : locationOptions.itsLonLats)
    {
      fromWhereClause << ((n == 0) ? " " : ")),") << "(ST_Point(" << lonlat.itsLon << ","
                      << lonlat.itsLat;
      n++;
    }

    fromWhereClause << "))) AS coordinates (" << stationCoordinateColumn
                    << ") WHERE ST_DWithin(geom::geography,ST_SetSRID(coordinates."
                    << stationCoordinateColumn << ",4326)::geography," << setprecision(0)
                    << locationOptions.itsMaxDistance << ")";

    // Note: When querying for given max # of nearest stations, stations having certain icaos codes
    // (starting with 'IL') are ignored if messages of the only message type (AWSMETAR) originating
    // from them will be ignored when querying messages (when messages types were given but AWSMETAR
    // was not included).
    //
    // This is to exclude nonapplicable stations when querying for TAFs, METARs etc

    const char* messageTypeSpecialToNearestStationSearch = "AWSMETAR";
    const char* stationIcaosSpecialToNearestStationSearch = "UPPER(icao_code) NOT LIKE 'IL%'";

    if ((locationOptions.itsNumberOfNearestStations > 0) && (!messageTypes.empty()) &&
        (find(messageTypes.begin(), messageTypes.end(), messageTypeSpecialToNearestStationSearch) ==
         messageTypes.end()))
      fromWhereClause << " AND (" << stationIcaosSpecialToNearestStationSearch << ")";
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build where clause with given station id's for querying stations
 */
// ----------------------------------------------------------------------

void buildStationQueryWhereClause(const StationIdList& stationIdList, ostringstream& whereClause)
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
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build where clause with given icao codes, places (station names) or country codes
 *        for querying stations, excluding given icao codes
 */
// ----------------------------------------------------------------------

void buildStationQueryWhereClause(const Fmi::Database::PostgreSQLConnection& connection,
                                  const string& columnExpression,
                                  const StringList& stringList,
                                  const string& filterColumnExpression,
                                  const StringList& filterStringList,
                                  ostringstream& whereClause)
{
  try
  {
    if (stringList.empty())
      return;

    whereClause << (whereClause.str().empty() ? "WHERE ((" : " OR ((");

    size_t n = 0;

    whereClause << columnExpression << " IN (";

    for (auto const& str : stringList)
    {
      whereClause << (n ? "," : "") << "UPPER(" << connection.quote(str) << ")";
      n++;
    }

    whereClause << "))";

    if (!filterStringList.empty())
    {
      n = 0;

      whereClause << " AND " << filterColumnExpression << " NOT ILIKE ALL (ARRAY[";

      for (auto const& str : filterStringList)
      {
        whereClause << (n ? "," : "") << connection.quote(str + ((str.size() < 4) ? "%" : ""));
        n++;
      }

      whereClause << "])";
    }

    whereClause << ")";
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build from and where (and order by for route query) clause with given wkts for querying
 *        stations
 */
// ----------------------------------------------------------------------

void buildStationQueryFromWhereOrderByClause(const Fmi::Database::PostgreSQLConnection& connection,
                                             const LocationOptions& locationOptions,
                                             const StringList& messageTypeList,
                                             const MessageTypes& knownMessageTypes,
                                             ostringstream& fromWhereOrderByClause)
{
  try
  {
    if (locationOptions.itsWKTs.itsWKTs.empty())
      return;

    StringList allMessageTypes = {""};
    const StringList& messageTypes = (messageTypeList.empty() ? allMessageTypes : messageTypeList);

    // Determine message scope (station for nonroute query) for station query generation.
    // All given message types must have same scope (station, fir or global) for route query

    MessageScope scope =
        (locationOptions.itsWKTs.isRoute ? MessageScope::NoScope : MessageScope::StationScope);

    if (scope == MessageScope::NoScope)
    {
      for (auto const& messageType : messageTypes)
      {
        MessageScope knownScope = MessageScope::NoScope;

        for (auto const& knownType : knownMessageTypes)
          if (messageType.empty() || (knownType == messageType))
          {
            knownScope = knownType.getScope();

            if (scope != MessageScope::NoScope)
            {
              if (knownScope != scope)
                throw Fmi::Exception::Trace(
                    BCP, "All requested message types must have same scope for route query");
            }
            else
              scope = knownScope;

            if (!messageType.empty())
              break;
          }

        if (knownScope == MessageScope::NoScope)
          throw Fmi::Exception::Trace(
              BCP, "buildStationQueryFromWhereOrderByClause: internal: message scope unknown");
      }
    }

    string geom;

    if (scope == MessageScope::StationScope)
      geom = string(stationTableAlias) + ".geom";
    else if (scope == MessageScope::FIRScope)
      geom = string(firTableAlias) + ".areageom";
    else
      return;

    fromWhereOrderByClause << " FROM " << stationTableName << " " << stationTableAlias;

    if (locationOptions.itsWKTs.isRoute)
    {
      // For route query the route segments (stations shortest distance to them) are used for
      // selecting the stations, and segment indexes and starting points (stations distance to them)
      // are used for ordering the stations along the route
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
      if (scope == MessageScope::FIRScope)
        fromWhereOrderByClause << "," << firTableName << " " << firTableAlias;

      fromWhereOrderByClause
          << ",(SELECT segindex,segstart,ST_SetSRID(ST_MakeLine(segstart,segend),4326) as segment "
          << "FROM (SELECT ST_PointN(route,generate_series(1,ST_NPoints(route)-1)) as segstart,"
          << "ST_PointN(route,generate_series(2,ST_NPoints(route))) as segend,"
          << "generate_series(1,ST_NPoints(route)-1) as segindex "
          << "FROM (SELECT '" << locationOptions.itsWKTs.itsWKTs.front()
          << "'::geometry as route) AS route) AS segpoints) AS segments";
    }

    fromWhereOrderByClause << " WHERE ((";

    size_t n = 0;

    for (auto const& wkt : locationOptions.itsWKTs.itsWKTs)
    {
      ostringstream condition;

      if (locationOptions.itsWKTs.isRoute)
      {
        // For a route (a single linestring), limit the stations by their shortest distance to route
        // segments
        //
        condition << "ST_DWithin(" << geom << "::geography,ST_ClosestPoint(segment," << geom
                  << ")::geography," << fixed << setprecision(0) << locationOptions.itsMaxDistance
                  << ")";

        if (scope == MessageScope::FIRScope)
          condition << " AND ST_Within(" << stationTableAlias << ".geom," << firTableAlias
                    << ".areageom)";
      }
      else
        // Limit by distance between geometries
        //
        condition << "ST_Length(ST_ShortestLine(geom,ST_GeogFromText('SRID=4326;" << wkt
                  << "')::geometry)::geography) <= " << fixed << setprecision(0)
                  << locationOptions.itsMaxDistance;

      fromWhereOrderByClause << ((n == 0) ? "(" : ") OR (") << condition.str();

      n++;
    }

    fromWhereOrderByClause << "))";

    if (!locationOptions.itsWKTs.isRoute)
    {
      // BRAINSTORM-3288
      //
      // Since edr converts collection/location/all query to area query and e.g. METAR and TAF
      // collection metadata is currently queried with countries="FI", the same filters i.e.
      // country (include) and icao code (exclude, e.g. ["IL", "EFIN"]) filters must be applied
      // when querying messages with polygon to restrict the stations like metadata query does
      // (otherwise e.g. swedish stations are included by the query)
      //
      // For edr SIGMET collection icao code filter (e.g. "EFIN") must be applied
      //
      if (!locationOptions.itsIncludeCountryFilters.empty())
      {
        n = 0;

        fromWhereOrderByClause << " AND " << stationTableAlias << "."
                               << stationCountryCodeTableColumn << " ILIKE ALL (ARRAY[";

        for (auto const& str : locationOptions.itsIncludeCountryFilters)
        {
          fromWhereOrderByClause << (n ? "," : "") << connection.quote(str);
          n++;
        }

        fromWhereOrderByClause << "])";
      }

      if (!locationOptions.itsIncludeIcaoFilters.empty())
      {
        n = 0;

        fromWhereOrderByClause << " AND " << stationTableAlias << "." << stationIcaoTableColumn
                               << " ILIKE ALL (ARRAY[";

        for (auto const& str : locationOptions.itsIncludeIcaoFilters)
        {
          fromWhereOrderByClause << (n ? "," : "")
                                 << connection.quote(str + ((str.size() < 4) ? "%" : ""));
          n++;
        }

        fromWhereOrderByClause << "])";
      }

      if (!locationOptions.itsExcludeIcaoFilters.empty())
      {
        n = 0;

        fromWhereOrderByClause << " AND " << stationTableAlias << "." << stationIcaoTableColumn
                               << " NOT ILIKE ALL (ARRAY[";

        for (auto const& str : locationOptions.itsExcludeIcaoFilters)
        {
          fromWhereOrderByClause << (n ? "," : "")
                                 << connection.quote(str + ((str.size() < 4) ? "%" : ""));
          n++;
        }

        fromWhereOrderByClause << "])";
      }
    }

    fromWhereOrderByClause << ")";

    // For a route (a single linestring), order the stations by route segment index and station's
    // distance to the start of the segment

    if (locationOptions.itsWKTs.isRoute)
      fromWhereOrderByClause << " ORDER BY segindex,ST_Distance(segstart::geography," << geom
                             << "::geography)";
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build where clause with given bboxes for querying stations
 */
// ----------------------------------------------------------------------

void buildStationQueryWhereClause(const BBoxList& bboxList,
                                  double maxDistance,
                                  ostringstream& whereClause,
                                  const string& whereOrEmpty = "WHERE ",
                                  const string& geomTableAlias = "")
{
  try
  {
    if (bboxList.empty())
      return;

    whereClause << (whereClause.str().empty() ? whereOrEmpty : " OR ");

    string geom(geomTableAlias.empty() ? "geom" : (geomTableAlias + ".geom"));
    size_t n = 0;

    for (auto const& bbox : bboxList)
    {
      ostringstream condition;

      condition << "(ST_Length(ST_ShortestLine(" << geom << ",ST_SetSRID(ST_MakeBox2D(ST_Point("
                << setprecision(10) << bbox.itsWest << "," << bbox.itsSouth << "),ST_Point("
                << bbox.itsEast << "," << bbox.itsNorth << ")),4326))::geography) <= " << fixed
                << setprecision(0) << maxDistance << ")";

      whereClause << ((n == 0) ? "" : " OR ") << condition.str();

      n++;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build where clause for station id's for message query
 */
// ----------------------------------------------------------------------

void buildMessageQueryWhereStationIdInClause(const StationIdList& stationIdList,
                                             ostringstream& whereClause)
{
  try
  {
    if (stationIdList.empty())
      return;

    // { WHERE | AND } me.station_id IN (stationIdList)

    string whereStationIdIn = (string(whereClause.str().empty() ? " WHERE " : " AND ") +
                               messageTableAlias + ".station_id IN (");
    size_t n = 0;

    for (auto stationId : stationIdList)
    {
      whereClause << ((n == 0) ? whereStationIdIn : ",") << stationId;
      n++;
    }

    whereClause << ")";
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build 'request_stations' table (WITH clause) for request station id's
 *        (and position/order) for message query
 */
// ----------------------------------------------------------------------

string buildRequestStationsWithClause(const StationIdList& stationIdList, bool routeQuery)
{
  try
  {
    /*
    WITH request_stations AS (
            SELECT request_stations.station_id[,request_stations.position]
            FROM (VALUES ((),...)) AS request_stations (station_id[,position])
    )
    */

    ostringstream withClause;

    withClause << "WITH " << requestStationsTable.itsName
               << " AS (SELECT request_stations.station_id"
               << (routeQuery ? string(",request_stations.") + requestStationsPositionColumn : "")
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
               << (routeQuery ? string(",") + requestStationsPositionColumn : "") << "))";

    return withClause.str();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build 'message_validity' table (WITH clause) for requested message
 *        types and their configured validity in hours
 */
// ----------------------------------------------------------------------

string buildMessageTypeValidityWithClause(const StringList& messageTypeList,
                                          const MessageTypes& knownMessageTypes)
{
  try
  {
    /*
    WITH message_validity AS (
            SELECT message_validity.type,message_validity.validityhours
            FROM (VALUES ((),...)) AS message_validity (type,validityhours)
    )
    */

    ostringstream withClause;

    withClause << messageValidityTable.itsName
               << " AS (SELECT message_validity.type,message_validity.validityhours"
               << " FROM (VALUES ";

    size_t n = 0;

    if (messageTypeList.empty())
    {
      // Get all types with configured validity hours
      //
      for (auto const& knownType : knownMessageTypes)
        if (knownType.hasValidityHours())
        {
          auto const& knownTypes = knownType.getMessageTypes();

          for (auto it = knownTypes.begin(); (it != knownTypes.end()); it++, n++)
            withClause << ((n == 0) ? "('" : "),('") << *it << "',INTERVAL '"
                       << knownType.getValidityHours() << " hours'";
        }
    }
    else
    {
      // Get given message types with configured validity hours
      //
      for (auto const& messageType : messageTypeList)
        for (auto const& knownType : knownMessageTypes)
          if (knownType.hasValidityHours() && (knownType == messageType))
          {
            withClause << ((n == 0) ? "('" : "),('") << messageType << "',INTERVAL '"
                       << knownType.getValidityHours() << " hours'";
            n++;

            break;
          }
    }

    withClause << ")) AS message_validity (type,validityhours))";

    return ((n > 0) ? withClause.str() : "");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Extract message types by scope
 */
// ----------------------------------------------------------------------

void scopeMessageTypes(const StringList& messageTypeList,
                       const MessageTypes& knownMessageTypes,
                       const list<MessageScope>& messageScopes,
                       StringList& scopeMessageTypeList)
{
  try
  {
    bool getAll = messageScopes.empty();

    scopeMessageTypeList.clear();

    if (messageTypeList.empty())
    {
      // Get all message types or all types matching messageScope(s)
      //
      for (auto const& knownType : knownMessageTypes)
        if (getAll || (find(messageScopes.begin(), messageScopes.end(), knownType.getScope()) !=
                       messageScopes.end()))
        {
          auto const& knownTypes = knownType.getMessageTypes();
          scopeMessageTypeList.insert(
              scopeMessageTypeList.begin(), knownTypes.begin(), knownTypes.end());
        }
    }
    else
    {
      // Get all given message types or given types matching messageScope(s)
      //
      for (auto const& messageType : messageTypeList)
        for (auto const& knownType : knownMessageTypes)
          if ((knownType == messageType) &&
              (getAll || (find(messageScopes.begin(), messageScopes.end(), knownType.getScope()) !=
                          messageScopes.end())))
          {
            scopeMessageTypeList.push_back(messageType);
            break;
          }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void scopeMessageTypes(const StringList& messageTypeList,
                       const MessageTypes& knownMessageTypes,
                       MessageScope messageScope,
                       StringList& scopeMessageTypeList)
{
  try
  {
    list<MessageScope> messageScopes;

    // If no scope, get all message types by passing on empty list

    if (messageScope != MessageScope::NoScope)
      messageScopes.push_back(messageScope);

    return scopeMessageTypes(
        messageTypeList, knownMessageTypes, messageScopes, scopeMessageTypeList);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build message type IN clause with given message and time range type(s)
 */
// ----------------------------------------------------------------------

string buildMessageTypeInClause(const StringList& messageTypeList,
                                const MessageTypes& knownMessageTypes,
                                list<TimeRangeType> timeRangeTypes)
{
  try
  {
    // mt.type IN (messageTypeList)

    ostringstream whereClause;
    string messageTypeIn = (string("UPPER(") + messageTypeTableAlias + ".type) IN ('");
    size_t n = 0;
    bool getAll = timeRangeTypes.empty();

    if (messageTypeList.empty())
    {
      // Get all message types or all types matching timeRangeType(s)
      //
      for (auto const& knownType : knownMessageTypes)
        if (getAll ||
            (find(timeRangeTypes.begin(), timeRangeTypes.end(), knownType.getTimeRangeType()) !=
             timeRangeTypes.end()))
        {
          auto const& knownTypes = knownType.getMessageTypes();

          for (auto it = knownTypes.begin(); (it != knownTypes.end()); it++, n++)
            whereClause << ((n == 0) ? messageTypeIn : "','") << *it;
        }
    }
    else
    {
      // Get all given message types or given types matching timeRangeType(s)
      //
      for (auto const& messageType : messageTypeList)
        for (auto const& knownType : knownMessageTypes)
          if ((knownType == messageType) &&
              (getAll ||
               (find(timeRangeTypes.begin(), timeRangeTypes.end(), knownType.getTimeRangeType()) !=
                timeRangeTypes.end())))
          {
            whereClause << ((n == 0) ? messageTypeIn : "','") << messageType;
            n++;

            break;
          }
    }

    whereClause << "')";

    return ((n > 0) ? whereClause.str() : "");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

string buildMessageTypeInClause(const StringList& messageTypeList,
                                const MessageTypes& knownMessageTypes,
                                TimeRangeType timeRangeType)
{
  try
  {
    list<TimeRangeType> timeRangeTypes;
    timeRangeTypes.push_back(timeRangeType);

    return buildMessageTypeInClause(messageTypeList, knownMessageTypes, timeRangeTypes);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build 'record_set' table (WITH clause) for querying accepted messages
 *        for given observation time or time range
 */
// ----------------------------------------------------------------------

string buildRecordSetWithClause(bool bboxQuery,
                                bool routeQuery,
                                const StationIdList& stationIdList,
                                const string& messageFormat,
                                const StringList& messageTypeList,
                                const MessageTypes& knownMessageTypes,
                                unsigned int startTimeOffsetHours,
                                unsigned int endTimeOffsetHours,
                                const string& obsOrRangeStartTime,
                                const string& rangeEndTimeOrEmpty = "")
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

    ostringstream withClause;
    string whereStationIdIn;

    if ((!bboxQuery) && (!stationIdList.empty()))
      if (!routeQuery)
        buildMessageQueryWhereStationIdInClause(stationIdList, withClause);
      else
        withClause << " WHERE " << messageTableAlias << ".station_id IN (SELECT station_id FROM "
                   << requestStationsTable.itsName << ")";
    else
      withClause << " WHERE ";

    whereStationIdIn = withClause.str();
    withClause.str("");
    withClause.clear();

    withClause << recordSetTableName << " AS (SELECT me.* FROM " << messageTableName << " "
               << messageTableAlias << "," << messageFormatTableName << " "
               << messageFormatTableAlias;

    if (!messageTypeList.empty())
      withClause << "," << messageTypeTableName << " " << messageTypeTableAlias;

    withClause << whereStationIdIn << ((!bboxQuery) && (!stationIdList.empty()) ? " AND " : "")
               << (messageFormat == "TAC" ? messageFormatTableJoinTAC
                                          : messageFormatTableJoinIWXXM);

    if (!messageTypeList.empty())
    {
      string messageTypeIn =
          buildMessageTypeInClause(messageTypeList, knownMessageTypes, list<TimeRangeType>());

      withClause << " AND " << messageTypeTableJoin << " AND " << messageTypeIn;
    }

    const string& obsOrRangeEndTime =
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
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build GROUP BY expression for message type with given message types
 *        (e.g. METREP,SPECIAL) and time range type for querying latest accepted messages
 */
// ----------------------------------------------------------------------

string buildMessageTypeGroupByExpr(const StringList& messageTypeList,
                                   const MessageTypes& knownMessageTypes,
                                   TimeRangeType timeRangeType)
{
  try
  {
    // [CASE mt.type WHEN type1 OR type2 [ .. ] THEN type1 [ WHEN .. ] ELSE ] mt.type [ END ]

    ostringstream groupBy;
    string whenMessageTypeIn = string(" WHEN UPPER(") + messageTypeTableAlias + ".type) IN ('";
    size_t n = 0;

    groupBy << "CASE";

    if (messageTypeList.empty())
    {
      // Get all message types matching timeRangeType
      //
      for (auto const& knownType : knownMessageTypes)
        if (knownType.getTimeRangeType() == timeRangeType)
        {
          auto const& knownTypes = knownType.getMessageTypes();

          if (knownTypes.size() > 1)
          {
            size_t nn = 0;

            for (auto it = knownTypes.begin(); (it != knownTypes.end()); it++, n++, nn++)
              groupBy << ((nn == 0) ? whenMessageTypeIn : "','") << *it;

            groupBy << "') THEN '" << knownTypes.front() << "'";
          }
        }
    }
    else
    {
      // Get given message types matching timeRangeType
      //
      StringList handledMessageTypes;

      for (auto const& messageType : messageTypeList)
        if (find(handledMessageTypes.begin(), handledMessageTypes.end(), messageType) ==
            handledMessageTypes.end())
          for (auto const& knownType : knownMessageTypes)
            if (knownType.getTimeRangeType() == timeRangeType)
            {
              // If all group's types are given, return the latest message for the group
              //
              auto const& knownTypes = knownType.getMessageTypes();

              if ((knownTypes.size() > 1) && (knownType == messageType))
              {
                auto it = knownTypes.begin();

                for (; (it != knownTypes.end()); it++)
                  if (find(messageTypeList.begin(), messageTypeList.end(), *it) ==
                      messageTypeList.end())
                    break;

                if (it == knownTypes.end())
                {
                  size_t nn = 0;

                  for (auto it = knownTypes.begin(); (it != knownTypes.end()); it++, n++, nn++)
                    groupBy << ((nn == 0) ? whenMessageTypeIn : "','") << *it;

                  groupBy << "') THEN '" << knownTypes.front() << "'";

                  handledMessageTypes.insert(
                      handledMessageTypes.end(), knownTypes.begin(), knownTypes.end());

                  break;
                }
              }
            }
    }

    if (n > 0)
      groupBy << " ELSE " << messageTypeTableAlias << ".type END";

    return ((n > 0) ? groupBy.str() : (string(messageTypeTableAlias) + ".type"));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build GROUP BY expression for messir_heading with given message types
 *        (e.g. GAFOR) and time range type for querying latest accepted messages
 */
// ----------------------------------------------------------------------

string buildMessirHeadingGroupByExpr(const StringList& messageTypeList,
                                     const MessageTypes& knownMessageTypes,
                                     TimeRangeType timeRangeType)
{
  try
  {
    // [
    //	CASE WHEN UPPER(mt.type) = 'type1'
    //			THEN CASE WHEN me.messir_heading LIKE 't1pat1' THEN 1 [WHEN
    // me.messir_heading
    // LIKE
    //'t1pat2' THEN 2 ... ] ELSE 0 END
    //	   [ WHEN UPPER(mt.type) = 'type2'
    //			...
    //		 ...
    //	   ]
    //		 ELSE 0
    //	END
    // ]

    ostringstream groupBy;
    string whenMessageTypeIs = string(" WHEN UPPER(") + messageTypeTableAlias + ".type) = '";
    string whenMessirHeadingLike =
        string(" WHEN UPPER(") + messageTableAlias + ".messir_heading) LIKE '";
    size_t n = 0;

    groupBy << ",CASE";

    if (messageTypeList.empty())
    {
      // Get all message types matching timeRangeType
      //
      for (auto const& knownType : knownMessageTypes)
        if (timeRangeType == knownType.getTimeRangeType())
        {
          auto const& knownTypes = knownType.getMessageTypes();
          auto const& messirPatterns = knownType.getMessirPatterns();

          if (!messirPatterns.empty())
          {
            // We don't expect/support 'grouped' messagetypes (e.g. METREP,SPECIAL) currently
            //
            size_t nn = 1;

            groupBy << whenMessageTypeIs << knownTypes.front() << "' THEN CASE";

            for (auto it = messirPatterns.begin(); (it != messirPatterns.end()); it++, nn++)
              groupBy << whenMessirHeadingLike << *it << "' THEN " << nn;

            groupBy << " ELSE 0 END";
            n++;
          }
        }
    }
    else
    {
      // Get given message types matching timeRangeType
      //
      for (auto const& messageType : messageTypeList)
        for (auto const& knownType : knownMessageTypes)
          if (knownType.getTimeRangeType() == timeRangeType)
          {
            if (knownType.getMessageTypes().front() == messageType)
            {
              auto const& messirPatterns = knownType.getMessirPatterns();

              if (!messirPatterns.empty())
              {
                size_t nn = 1;

                groupBy << whenMessageTypeIs << messageType << "' THEN CASE";

                for (auto it = messirPatterns.begin(); (it != messirPatterns.end()); it++, nn++)
                  groupBy << whenMessirHeadingLike << *it << "' THEN " << nn;

                groupBy << " ELSE 0 END";
                n++;
              }
            }
          }
    }

    if (n > 0)
      groupBy << " ELSE 0 END";

    return ((n > 0) ? groupBy.str() : "");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build WHERE time and icao and country code(s) (when configured) conditions
 *        for querying latest accepted messages for messagetypes having
 *        MessageValidTimeRangeLatest time restriction (i.e. TAF)
 */
// ----------------------------------------------------------------------

string buildMessageValidTimeRangeLatestTimeCondition(const StringList& messageTypeList,
                                                     const MessageTypes& knownMessageTypes,
                                                     const string& observationTime,
                                                     const string& messageCreatedTime,
                                                     bool& bStationJoin)
{
  try
  {
    ostringstream whereExpr;

    // BRAINSTORM-3300, BRAINSTORM-3301
    //
    // No (effective) message creation time check or query time restriction for edr -queries
    //
    bool disableQueryRestriction = (!messageCreatedTime.empty());
    auto const& createdTime = (messageCreatedTime.empty() ? observationTime : messageCreatedTime);
    std::string noMatch("99");

    bStationJoin = false;

    if (messageTypeList.empty())
    {
      // Get all message types having MessageValidTimeRangeLatest time restriction
      //
      // Only one type (i.e. TAF) currently supported
      //
      for (auto const& knownType : knownMessageTypes)
        if (knownType.getTimeRangeType() == TimeRangeType::MessageValidTimeRangeLatest)
        {
          if ((!knownType.getQueryRestrictionHours().empty()) || bStationJoin)
          {
            // Messages (i.e. TAFs) are stored e.g. every n'th (3rd) hour between xx:20 and xx:40
            // and then published; during publication hour delay latest message until xx:40

            if (!(whereExpr.str().empty()))
              throw Fmi::Exception::Trace(
                  BCP, "Query time restriction settings not supported for multiple messagetypes");

            whereExpr << "(((" << observationTime << " >= " << messageTableAlias
                      << ".message_time AND " << observationTime << " < " << messageTableAlias
                      << ".valid_to) AND ((";

            auto icaoPatterns = knownType.getQueryRestrictionIcaoPatterns();

            for (auto const& pattern : icaoPatterns)
              whereExpr << ((&pattern == &icaoPatterns.front()) ? "" : " AND ") << "(UPPER("
                        << stationTableAlias << "." << stationIcaoTableColumn << ") NOT LIKE '"
                        << pattern << "')";

            auto countryCodes = knownType.getQueryRestrictionCountryCodes();

            for (auto const& code : countryCodes)
              whereExpr << (((&code == &countryCodes.front()) && icaoPatterns.empty()) ? ""
                                                                                       : " AND ")
                        << "(UPPER(" << stationTableAlias << "." << stationCountryCodeTableColumn
                        << ") != '" << code << "')";

            auto const& queryRestrictionHours =
                (disableQueryRestriction ? noMatch : knownType.getQueryRestrictionHours());

            whereExpr << ") OR (DATE_TRUNC('hour'," << messageTableAlias
                      << ".message_time) != DATE_TRUNC('hour'," << observationTime << ")) OR "
                      << "(EXTRACT(HOUR FROM " << observationTime << ") NOT IN ("
                      << queryRestrictionHours << ")) OR "
                      << "(EXTRACT(MINUTE FROM " << messageTableAlias << ".message_time) < "
                      << knownType.getQueryRestrictionStartMinute() << ") OR "
                      << "(EXTRACT(MINUTE FROM " << observationTime
                      << ") >= " << knownType.getQueryRestrictionEndMinute() << "))) OR ("
                      << messageTableAlias << ".valid_from IS NULL AND " << messageTableAlias
                      << ".valid_to IS NULL AND " << observationTime << " >= " << messageTableAlias
                      << ".message_time AND " << observationTime << " < (" << messageTableAlias
                      << ".message_time + " << messageValidityTableAlias << ".validityhours))) AND "
                      << createdTime << " >= " << messageTableAlias << ".created";

            bStationJoin = true;
          }
          else if (whereExpr.str().empty())
            whereExpr << "((" << observationTime << " BETWEEN " << messageTableAlias
                      << ".message_time AND " << messageTableAlias << ".valid_to)"
                      << " OR (" << messageTableAlias << ".valid_from IS NULL AND "
                      << messageTableAlias << ".valid_to IS NULL AND " << observationTime
                      << " >= " << messageTableAlias << ".message_time AND " << observationTime
                      << " < (" << messageTableAlias << ".message_time + "
                      << messageValidityTableAlias << ".validityhours))) AND " << createdTime
                      << " >= " << messageTableAlias << ".created";
        }

      return whereExpr.str();
    }
    else
    {
      // Get given message types having MessageValidTimeRangeLatest time restriction
      //
      for (auto const& messageType : messageTypeList)
        for (auto const& knownType : knownMessageTypes)
          if (knownType.getTimeRangeType() == TimeRangeType::MessageValidTimeRangeLatest)
          {
            if (knownType.getMessageTypes().front() == messageType)
            {
              if ((!knownType.getQueryRestrictionHours().empty()) || bStationJoin)
              {
                if (!(whereExpr.str().empty()))
                  throw Fmi::Exception::Trace(
                      BCP,
                      "Query time restriction settings not supported for multiple messagetypes");

                whereExpr << "(((" << observationTime << " >= " << messageTableAlias
                          << ".message_time AND " << observationTime << " < " << messageTableAlias
                          << ".valid_to) AND ((";

                auto icaoPatterns = knownType.getQueryRestrictionIcaoPatterns();

                for (auto const& pattern : icaoPatterns)
                  whereExpr << ((&pattern == &icaoPatterns.front()) ? "" : " AND ") << "(UPPER("
                            << stationTableAlias << "." << stationIcaoTableColumn << ") NOT LIKE '"
                            << pattern << "')";

                auto countryCodes = knownType.getQueryRestrictionCountryCodes();

                for (auto const& code : countryCodes)
                  whereExpr << (((&code == &countryCodes.front()) && icaoPatterns.empty())
                                    ? ""
                                    : " AND ")
                            << "(UPPER(" << stationTableAlias << "."
                            << stationCountryCodeTableColumn << ") != '" << code << "')";

                auto const& queryRestrictionHours =
                    (disableQueryRestriction ? noMatch : knownType.getQueryRestrictionHours());

                whereExpr << ") OR (DATE_TRUNC('hour'," << messageTableAlias
                          << ".message_time) != DATE_TRUNC('hour'," << observationTime << ")) OR "
                          << "(EXTRACT(HOUR FROM " << observationTime << ") NOT IN ("
                          << queryRestrictionHours << ")) OR "
                          << "(EXTRACT(MINUTE FROM " << messageTableAlias << ".message_time) < "
                          << knownType.getQueryRestrictionStartMinute() << ") OR "
                          << "(EXTRACT(MINUTE FROM " << observationTime
                          << ") >= " << knownType.getQueryRestrictionEndMinute() << "))) OR ("
                          << messageTableAlias << ".valid_from IS NULL AND " << messageTableAlias
                          << ".valid_to IS NULL AND " << observationTime
                          << " >= " << messageTableAlias << ".message_time AND " << observationTime
                          << " < (" << messageTableAlias << ".message_time + "
                          << messageValidityTableAlias << ".validityhours))) AND " << createdTime
                          << " >= " << messageTableAlias << ".created";

                bStationJoin = true;
              }
              else if (whereExpr.str().empty())
                whereExpr << "((" << observationTime << " BETWEEN " << messageTableAlias
                          << ".message_time AND " << messageTableAlias << ".valid_to)"
                          << " OR (" << messageTableAlias << ".valid_from IS NULL AND "
                          << messageTableAlias << ".valid_to IS NULL AND " << observationTime
                          << " >= " << messageTableAlias << ".message_time AND " << observationTime
                          << " < (" << messageTableAlias << ".message_time + "
                          << messageValidityTableAlias << ".validityhours))) AND " << createdTime
                          << " >= " << messageTableAlias << ".created";

              break;
            }
          }
    }

    if (whereExpr.str().empty())
      throw Fmi::Exception::Trace(BCP, "Internal error, no matching messagetype");

    return whereExpr.str();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build WHERE time and icao and country code(s) (when configured) conditions
 *        for querying accepted messages within time range for messagetypes having
 *        MessageValidTimeRange[Latest] time restriction (i.e. TAF)
 */
// ----------------------------------------------------------------------

string buildMessageValidTimeRangeTimeCondition(const StringList& messageTypeList,
                                               const MessageTypes& knownMessageTypes,
                                               const string& startTime,
                                               const string& endTime,
                                               bool disableQueryRestriction,
                                               bool& bStationJoin)
{
  try
  {
    ostringstream whereExpr;
    std::string noMatch("99");

    bStationJoin = false;

    if (messageTypeList.empty())
    {
      // Get all message types having MessageValidTimeRangeLatest time restriction
      //
      // Only one type (i.e. TAF) currently supported
      //
      for (auto const& knownType : knownMessageTypes)
        if (knownType.getTimeRangeType() == TimeRangeType::MessageValidTimeRangeLatest)
        {
          if ((!knownType.getQueryRestrictionHours().empty()) || bStationJoin)
          {
            // Messages (i.e. TAFs) are stored e.g. every n'th (3rd) hour between xx:20 and xx:40
            // and then published; ignore publication hour's messages unless xx:40 within range

            if (!(whereExpr.str().empty()))
              throw Fmi::Exception::Trace(
                  BCP, "Query time restriction settings not supported for multiple messagetypes");

            whereExpr << "(((" << startTime << " < " << messageTableAlias << ".valid_to AND "
                      << endTime << " > " << messageTableAlias << ".message_time) AND ((";

            auto icaoPatterns = knownType.getQueryRestrictionIcaoPatterns();

            for (auto const& pattern : icaoPatterns)
              whereExpr << ((&pattern == &icaoPatterns.front()) ? "" : " AND ") << "(UPPER("
                        << stationTableAlias << "." << stationIcaoTableColumn << ") NOT LIKE '"
                        << pattern << "')";

            auto countryCodes = knownType.getQueryRestrictionCountryCodes();

            for (auto const& code : countryCodes)
              whereExpr << (((&code == &countryCodes.front()) && icaoPatterns.empty()) ? ""
                                                                                       : " AND ")
                        << "(UPPER(" << stationTableAlias << "." << stationCountryCodeTableColumn
                        << ") != '" << code << "')";

            // BRAINSTORM-3301
            //
            // No (effective) query time restriction for edr -queries
            //
            auto const& queryRestrictionHours =
                (disableQueryRestriction ? noMatch : knownType.getQueryRestrictionHours());

            whereExpr << ") OR (DATE_TRUNC('hour'," << messageTableAlias
                      << ".message_time) != DATE_TRUNC('hour'," << endTime << ")) OR "
                      << "(EXTRACT(HOUR FROM " << endTime << ") NOT IN (" << queryRestrictionHours
                      << ")) OR "
                      << "(EXTRACT(MINUTE FROM " << messageTableAlias << ".message_time) < "
                      << knownType.getQueryRestrictionStartMinute() << ") OR "
                      << "(EXTRACT(MINUTE FROM " << endTime
                      << ") >= " << knownType.getQueryRestrictionEndMinute() << "))) OR ("
                      << messageTableAlias << ".valid_from IS NULL AND " << messageTableAlias
                      << ".valid_to IS NULL AND "
                      << "(" << startTime << " < (" << messageTableAlias << ".message_time + "
                      << messageValidityTableAlias << ".validityhours)"
                      << " AND " << endTime << " > " << messageTableAlias << ".message_time)))";

            bStationJoin = true;
          }
          else if (whereExpr.str().empty())
            whereExpr << "((" << startTime << " < " << messageTableAlias << ".valid_to AND "
                      << endTime << " > " << messageTableAlias << ".message_time) OR ("
                      << messageTableAlias << ".valid_from IS NULL AND " << messageTableAlias
                      << ".valid_to IS NULL AND (" << startTime << " < (" << messageTableAlias
                      << ".message_time + " << messageValidityTableAlias << ".validityhours) AND "
                      << endTime << " > " << messageTableAlias << ".message_time)))";
        }

      return whereExpr.str();
    }
    else
    {
      // Get given message types having MessageValidTimeRangeLatest time restriction
      //
      for (auto const& messageType : messageTypeList)
        for (auto const& knownType : knownMessageTypes)
          if (knownType.getTimeRangeType() == TimeRangeType::MessageValidTimeRangeLatest)
          {
            if (knownType.getMessageTypes().front() == messageType)
            {
              if ((!knownType.getQueryRestrictionHours().empty()) || bStationJoin)
              {
                if (!(whereExpr.str().empty()))
                  throw Fmi::Exception::Trace(
                      BCP,
                      "Query time restriction settings not supported for multiple messagetypes");

                whereExpr << "(((" << startTime << " < " << messageTableAlias << ".valid_to AND "
                          << endTime << " > " << messageTableAlias << ".message_time) AND ((";

                auto icaoPatterns = knownType.getQueryRestrictionIcaoPatterns();

                for (auto const& pattern : icaoPatterns)
                  whereExpr << ((&pattern == &icaoPatterns.front()) ? "" : " AND ") << "(UPPER("
                            << stationTableAlias << "." << stationIcaoTableColumn << ") NOT LIKE '"
                            << pattern << "')";

                auto countryCodes = knownType.getQueryRestrictionCountryCodes();

                for (auto const& code : countryCodes)
                  whereExpr << (((&code == &countryCodes.front()) && icaoPatterns.empty())
                                    ? ""
                                    : " AND ")
                            << "(UPPER(" << stationTableAlias << "."
                            << stationCountryCodeTableColumn << ") != '" << code << "')";

                // BRAINSTORM-3301
                //
                // No (effective) query time restriction for edr -queries
                //
                auto const& queryRestrictionHours =
                    (disableQueryRestriction ? noMatch : knownType.getQueryRestrictionHours());

                whereExpr << ") OR (DATE_TRUNC('hour'," << messageTableAlias
                          << ".message_time) != DATE_TRUNC('hour'," << endTime << ")) OR "
                          << "(EXTRACT(HOUR FROM " << endTime << ") NOT IN ("
                          << queryRestrictionHours << ")) OR "
                          << "(EXTRACT(MINUTE FROM " << messageTableAlias << ".message_time) < "
                          << knownType.getQueryRestrictionStartMinute() << ") OR "
                          << "(EXTRACT(MINUTE FROM " << endTime
                          << ") >= " << knownType.getQueryRestrictionEndMinute() << "))) OR ("
                          << messageTableAlias << ".valid_from IS NULL AND " << messageTableAlias
                          << ".valid_to IS NULL AND "
                          << "(" << startTime << " < (" << messageTableAlias << ".message_time + "
                          << messageValidityTableAlias << ".validityhours)"
                          << " AND " << endTime << " > " << messageTableAlias << ".message_time)))";

                bStationJoin = true;
              }
              else if (whereExpr.str().empty())
                whereExpr << "((" << startTime << " < " << messageTableAlias << ".valid_to AND "
                          << endTime << " > " << messageTableAlias << ".message_time) OR ("
                          << messageTableAlias << ".valid_from IS NULL AND " << messageTableAlias
                          << ".valid_to IS NULL AND (" << startTime << " < (" << messageTableAlias
                          << ".message_time + " << messageValidityTableAlias
                          << ".validityhours) AND " << endTime << " > " << messageTableAlias
                          << ".message_time)))";

              break;
            }
          }
    }

    if (whereExpr.str().empty())
      throw Fmi::Exception::Trace(BCP, "Internal error, no matching messagetype");

    return whereExpr.str();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Get list<string> as 's1,'s2',...
 */
// ----------------------------------------------------------------------

string getStringList(const list<string>& stringList)
{
  try
  {
    string slist;

    for (auto const& s : stringList)
      slist.append(slist.empty() ? "'" : "','").append(s);

    return slist.empty() ? slist : (slist + "'");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build 'latest_messages' table (WITH clause) for querying latest
 *        accepted messages for given observation time
 */
// ----------------------------------------------------------------------

string buildLatestMessagesWithClause(const StringList& messageTypes,
                                     const MessageTypes& knownMessageTypes,
                                     const string& observationTime,
                                     const string& messageCreatedTime,
                                     bool filterFIMETARxxx,
                                     bool excludeFISPECI,
                                     bool distinct,
                                     const list<string>& filterFIMETARxxxExcludeIcaos)
{
  try
  {
    /*
    latest_messages AS (
             SELECT MAX(message_id) AS message_id
             FROM record_set me,avidb_message_types mt
             WHERE mt.type IN (ValidTimeRangeLatestTypes) AND
                       observation time BETWEEN me.valid_from AND me.valid_to AND observation time
    >=
    me.created
             GROUP BY me.station_id,[CASE mt.type WHEN type1 OR type2 [ .. ] THEN type1 [ WHEN .. ]
    ELSE ] mt.type [ END ]
                          [,CASE WHEN UPPER(mt.type) = 'type1' THEN CASE WHEN me.messir_heading LIKE
    't1pat1' THEN 1 [WHEN me.messir_heading LIKE 't1pat2' THEN 2 ... ] ELSE 0 END
                                       [ WHEN ... ]
                                         ELSE 0
                                    END]
             UNION ALL
             SELECT MAX(message_id) AS message_id
             FROM record_set me,avidb_message_types mt,message_validity mv
             WHERE mt.type IN (MessageValidTimeRangeLatestTypes) AND
                       mv.type = mt.type AND (
                        (observation time BETWEEN me.message_time AND me.valid_to) OR
                        (me.valid_from IS NULL AND me.valid_to IS NULL AND observation time BETWEEN
    me.message_time AND me.message_time + mv.validityhours)
                       ) AND
                       observation time >= me.created
             GROUP BY me.station_id,[CASE mt.type WHEN type1 OR type2 [ .. ] THEN type1 [ WHEN .. ]
    ELSE ] mt.type [ END ]
                          [,CASE WHEN UPPER(mt.type) = 'type1' THEN CASE WHEN me.messir_heading LIKE
    't1pat1' THEN 1 [WHEN me.messir_heading LIKE 't1pat2' THEN 2 ... ] ELSE 0 END
                                       [ WHEN ... ]
                                         ELSE 0
                                    END]
             UNION ALL
             SELECT MAX(message_id) AS message_id
             FROM record_set me,avidb_message_types mt,message_validity mv[,avidb_stations st]
             WHERE [ st.station_id = me.station_id AND ((st.country_code != 'FI' OR mt.type !=
    'METAR'
    OR me.message LIKE 'METAR%')
                             [ OR st.icao_code IN (ExcludedIcaoList) ]
                             ) AND
                       ]
                       mt.type IN (MessageTimeRangeLatestTypes) AND
                       mv.type = mt.type AND
                       observation time BETWEEN me.message_time AND me.message_time +
    mv.validityhours
    AND observation time >= me.created
             GROUP BY me.station_id,[CASE mt.type WHEN type1 OR type2 [ .. ] THEN type1 [ WHEN .. ]
    ELSE ] mt.type [ END ]
                          [,CASE WHEN UPPER(mt.type) = 'type1' THEN CASE WHEN me.messir_heading LIKE
    't1pat1' THEN 1 [WHEN me.messir_heading LIKE 't1pat2' THEN 2 ... ] ELSE 0 END
                                       [ WHEN ... ]
                                         ELSE 0
                                    END]
             UNION ALL
             SELECT MAX(message_id) AS message_id
             FROM record_set me,avidb_message_types mt
             WHERE mt.type IN (CreationValidTimeRangeLatestTypes) AND
                       observation time BETWEEN me.created AND me.valid_to
             GROUP BY me.station_id,[CASE mt.type WHEN type1 OR type2 [ .. ] THEN type1 [ WHEN .. ]
    ELSE ] mt.type [ END ]
                          [,CASE WHEN UPPER(mt.type) = 'type1' THEN CASE WHEN me.messir_heading LIKE
    't1pat1' THEN 1 [WHEN me.messir_heading LIKE 't1pat2' THEN 2 ... ] ELSE 0 END
                                       [ WHEN ... ]
                                         ELSE 0
                                    END]
             UNION ALL
             SELECT message_id
             FROM record_set me,avidb_message_types mt
             WHERE mt.type IN (ValidTimeRangeTypes) AND
                       observation time BETWEEN me.valid_from AND me.valid_to AND observation time
    >=
    me.created
             UNION ALL
             SELECT message_id
             FROM record_set me,avidb_message_types mt,message_validity mv
             WHERE mt.type IN (MessageTimeRangeTypes) AND
                       mv.type = mt.type AND
                       observation time BETWEEN me.message_time AND me.message_time +
    mv.validityhours
    AND observation time >= me.created
             UNION ALL
             SELECT message_id
             FROM record_set me,avidb_message_types mt
             WHERE mt.type IN (CreationValidTimeRangeTypes) AND
                       observation time BETWEEN creation_time AND me.valid_to
    )
    */

    ostringstream withClause;
    string unionOrEmpty = "";
    const string latestMessageIdQueryExpr =
        "DISTINCT first_value(me.message_id) OVER (PARTITION BY me.station_id,";
    const string latestMessageIdOrderByExpr = " ORDER BY me.message_time DESC,me.created DESC) ";

    auto const& createdTime = (messageCreatedTime.empty() ? observationTime : messageCreatedTime);

    withClause << latestMessagesTable.itsName << " AS (";

    string messageTypeIn = buildMessageTypeInClause(
        messageTypes, knownMessageTypes, TimeRangeType::ValidTimeRangeLatest);

    if (!messageTypeIn.empty())
    {
      // Depending on configuration the latest message for group(s) of types might be returned
      //
      string messageTypeGroupByExpr = buildMessageTypeGroupByExpr(
          messageTypes, knownMessageTypes, TimeRangeType::ValidTimeRangeLatest);

      // Depending on configuration messir_heading LIKE pattern(s) might be used for additional
      // grouping

      string messirHeadingGroupByExpr = buildMessirHeadingGroupByExpr(
          messageTypes, knownMessageTypes, TimeRangeType::ValidTimeRangeLatest);

      withClause << "SELECT " << latestMessageIdQueryExpr << messageTypeGroupByExpr
                 << messirHeadingGroupByExpr << latestMessageIdOrderByExpr << "AS message_id"
                 << " FROM record_set " << messageTableAlias << ",avidb_message_types mt"
                 << " WHERE " << messageTypeTableJoin << " AND " << messageTypeIn << " AND "
                 << observationTime << " >= " << messageTableAlias << ".valid_from AND "
                 << observationTime << " < " << messageTableAlias << ".valid_to"
                 << " AND " << createdTime << " >= " << messageTableAlias << ".created";

      unionOrEmpty = " UNION ALL ";
    }

    messageTypeIn = buildMessageTypeInClause(
        messageTypes, knownMessageTypes, TimeRangeType::MessageValidTimeRangeLatest);

    if (!messageTypeIn.empty())
    {
      string messirHeadingGroupByExpr = buildMessirHeadingGroupByExpr(
          messageTypes, knownMessageTypes, TimeRangeType::MessageValidTimeRangeLatest);

      bool bStationJoin;
      string stationAndTimeCondition = buildMessageValidTimeRangeLatestTimeCondition(
          messageTypes, knownMessageTypes, observationTime, messageCreatedTime, bStationJoin);

      withClause << unionOrEmpty << "SELECT " << latestMessageIdQueryExpr << "mt.type_id"
                 << (distinct ? "" : ", me.route_id") << messirHeadingGroupByExpr
                 << latestMessageIdOrderByExpr << "AS message_id"
                 << " FROM record_set " << messageTableAlias << ",avidb_message_types mt"
                 << (bStationJoin ? (string(",") + stationTableName + " " + stationTableAlias) : "")
                 << "," << messageValidityTable.itsName << " " << messageValidityTableAlias
                 << " WHERE " << messageTypeTableJoin << " AND " << messageTypeIn
                 << (bStationJoin ? (string(" AND ") + stationTableJoin) : "") << " AND ("
                 << stationAndTimeCondition << ")";

      unionOrEmpty = " UNION ALL ";
    }

    messageTypeIn = buildMessageTypeInClause(
        messageTypes, knownMessageTypes, TimeRangeType::MessageTimeRangeLatest);

    if (!messageTypeIn.empty())
    {
      string messageTypeGroupByExpr = buildMessageTypeGroupByExpr(
          messageTypes, knownMessageTypes, TimeRangeType::MessageTimeRangeLatest);
      string messirHeadingGroupByExpr = buildMessirHeadingGroupByExpr(
          messageTypes, knownMessageTypes, TimeRangeType::MessageTimeRangeLatest);
      string whereOrAnd = " WHERE ";

      withClause << unionOrEmpty << "SELECT " << latestMessageIdQueryExpr << messageTypeGroupByExpr
                 << messirHeadingGroupByExpr << latestMessageIdOrderByExpr << "AS message_id"
                 << " FROM record_set " << messageTableAlias << ",avidb_message_types mt"
                 << "," << messageValidityTable.itsName << " " << messageValidityTableAlias;

      bool filterMETARs = filterFIMETARxxx && (messageTypeIn.find("'METAR") != string::npos);
      bool excludeSPECIs = excludeFISPECI && (messageTypeIn.find("'SPECI'") != string::npos);

      if (filterMETARs || excludeSPECIs)
      {
        withClause << "," << stationTableName << " " << stationTableAlias << " WHERE "
                   << stationTableJoin << " AND (st.country_code != 'FI' OR (";

        if (filterMETARs)
        {
          withClause << "((mt.type != 'METAR' OR " << messageTableAlias
                     << ".message LIKE 'METAR%')";

          if (!filterFIMETARxxxExcludeIcaos.empty())
            withClause << " OR st.icao_code IN (" << getStringList(filterFIMETARxxxExcludeIcaos)
                       << ")";

          withClause << ")";
        }

        if (excludeSPECIs)
          withClause << (filterMETARs ? " AND " : "") << "mt.type != 'SPECI'";

        withClause << "))";

        whereOrAnd = " AND ";
      }

      withClause << whereOrAnd << messageTypeTableJoin << " AND " << messageTypeIn << " AND "
                 << messageValidityTableJoin << " AND " << observationTime
                 << " >= " << messageTableAlias << ".message_time AND " << observationTime << " < ("
                 << messageTableAlias << ".message_time + " << messageValidityTableAlias
                 << ".validityhours) AND " << createdTime << " >= " << messageTableAlias
                 << ".created";

      unionOrEmpty = " UNION ALL ";
    }

    messageTypeIn = buildMessageTypeInClause(
        messageTypes, knownMessageTypes, TimeRangeType::CreationValidTimeRangeLatest);

    if (!messageTypeIn.empty())
    {
      string messirHeadingGroupByExpr = buildMessirHeadingGroupByExpr(
          messageTypes, knownMessageTypes, TimeRangeType::CreationValidTimeRangeLatest);

      withClause << unionOrEmpty << "SELECT " << latestMessageIdQueryExpr << "mt.type_id"
                 << messirHeadingGroupByExpr << latestMessageIdOrderByExpr << "AS message_id"
                 << " FROM record_set " << messageTableAlias << ",avidb_message_types mt"
                 << " WHERE " << messageTypeTableJoin << " AND " << messageTypeIn << " AND "
                 << createdTime << " >= " << messageTableAlias << ".created AND " << observationTime
                 << " < " << messageTableAlias << ".valid_to";

      unionOrEmpty = " UNION ALL ";
    }

    messageTypeIn =
        buildMessageTypeInClause(messageTypes, knownMessageTypes, TimeRangeType::ValidTimeRange);

    if (!messageTypeIn.empty())
    {
      withClause << unionOrEmpty << "SELECT message_id FROM record_set " << messageTableAlias
                 << ",avidb_message_types mt"
                 << " WHERE " << messageTypeTableJoin << " AND " << messageTypeIn << " AND "
                 << observationTime << " >= " << messageTableAlias << ".valid_from AND "
                 << observationTime << " < " << messageTableAlias << ".valid_to"
                 << " AND " << createdTime << " >= " << messageTableAlias << ".created";

      unionOrEmpty = " UNION ALL ";
    }

    messageTypeIn =
        buildMessageTypeInClause(messageTypes, knownMessageTypes, TimeRangeType::MessageTimeRange);

    if (!messageTypeIn.empty())
    {
      withClause << unionOrEmpty << "SELECT message_id FROM record_set " << messageTableAlias
                 << ",avidb_message_types mt"
                 << "," << messageValidityTable.itsName << " " << messageValidityTableAlias
                 << " WHERE " << messageTypeTableJoin << " AND " << messageTypeIn << " AND "
                 << messageValidityTableJoin << " AND " << observationTime
                 << " >= " << messageTableAlias << ".message_time AND " << observationTime << " < ("
                 << messageTableAlias << ".message_time + " << messageValidityTableAlias
                 << ".validityhours) AND " << createdTime << " >= " << messageTableAlias
                 << ".created";

      unionOrEmpty = " UNION ALL ";
    }

    messageTypeIn = buildMessageTypeInClause(
        messageTypes, knownMessageTypes, TimeRangeType::CreationValidTimeRange);

    if (!messageTypeIn.empty())
      withClause << unionOrEmpty << "SELECT message_id FROM record_set " << messageTableAlias
                 << ",avidb_message_types mt"
                 << " WHERE " << messageTypeTableJoin << " AND " << messageTypeIn << " AND "
                 << createdTime << " >= " << messageTableAlias << ".created AND " << observationTime
                 << " < " << messageTableAlias << ".valid_to";

    withClause << ")";

    return withClause.str();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build 'messagetimerangelatest_messages' table (WITH clause) for
 *        querying accepted messages having MessageTimeRangeLatest time
 *        restriction with given time range
 */
// ----------------------------------------------------------------------

string buildMessageTimeRangeMessagesWithClause(const StringList& messageTypes,
                                               const MessageTypes& knownMessageTypes,
                                               const string& startTime,
                                               const string& endTime,
                                               bool filterFIMETARxxx,
                                               bool excludeFISPECI,
                                               const list<string>& filterFIMETARxxxExcludeIcaos)
{
  try
  {
    /*
    messagetimerangelatest_messages AS (
            SELECT me.message_id
            FROM record_set me,avidb_message_types mt[,avidb_stations st]
            WHERE [ st.station_id = me.station_id AND ((st.country_code != 'FI' OR mt.type !=
    'METAR'
    OR " << messageTableAlias << ".message LIKE 'METAR%')
                            [ OR st.icao_code IN (ExcludedIcaoList) ]
                            ) AND
                      ]
                      me.type_id = mt.type_id AND
                      UPPER(mt.type) IN (MessageTimeRangeLatestTypes) AND
                      me.message_time >= starttime AND me.message_time < endtime
            UNION ALL
            SELECT MAX(message_id) AS message_id
            FROM record_set me,avidb_message_types mt,message_validity mv[,avidb_stations st]
            WHERE [ st.station_id = me.station_id AND ((st.country_code != 'FI' OR mt.type !=
    'METAR'
    OR " << messageTableAlias << ".message LIKE 'METAR%')
                            [ OR st.icao_code IN (ExcludedIcaoList) ]
                            ) AND
                      ]
                      me.type_id = mt.type_id AND
                      UPPER(mt.type) IN (MessageTimeRangeLatestTypes) AND
                      mv.type = mt.type AND
                      me.message_time < starttime AND me.message_time + mv.validityhours > starttime
            GROUP BY me.station_id,mt.type
    )

    Note: For grouped types (like METREP/SPECIAL, currently such don't exist with
    MessageTimeRangeLatest time restriction) latest message would be returned for each group's type
    */

    string messageTypeIn = buildMessageTypeInClause(
        messageTypes, knownMessageTypes, TimeRangeType::MessageTimeRangeLatest);

    if (messageTypeIn.empty())
      return "";

    ostringstream withClause, filterClause;
    string whereOrAnd = " WHERE ";

    withClause << "," << messageTimeRangeLatestMessagesTableName << " AS ("
               << "SELECT me.message_id "
               << "FROM record_set me,avidb_message_types mt";

    bool filterMETARs = filterFIMETARxxx && (messageTypeIn.find("'METAR") != string::npos);
    bool excludeSPECIs = excludeFISPECI && (messageTypeIn.find("'SPECI'") != string::npos);

    if (filterMETARs || excludeSPECIs)
    {
      filterClause << "," << stationTableName << " " << stationTableAlias << " WHERE "
                   << stationTableJoin << " AND (st.country_code != 'FI' OR (";

      if (filterMETARs)
      {
        filterClause << "((mt.type != 'METAR' OR " << messageTableAlias
                     << ".message LIKE 'METAR%')";

        if (!filterFIMETARxxxExcludeIcaos.empty())
          filterClause << " OR st.icao_code IN (" << getStringList(filterFIMETARxxxExcludeIcaos)
                       << ")";

        filterClause << ")";
      }

      if (excludeSPECIs)
        filterClause << (filterMETARs ? " AND " : "") << "mt.type != 'SPECI'";

      filterClause << "))";

      withClause << filterClause.str();

      whereOrAnd = " AND ";
    }

    withClause << whereOrAnd << "me.type_id = mt.type_id AND " << messageTypeIn
               << " AND me.message_time >= " << startTime << " AND me.message_time < " << endTime
               << " UNION "
               << "SELECT MAX(message_id) AS message_id "
               << "FROM record_set me,avidb_message_types mt,message_validity mv";

    if (filterMETARs || excludeSPECIs)
      withClause << filterClause.str();

    withClause << whereOrAnd << "me.type_id = mt.type_id AND " << messageTypeIn
               << " AND mv.type = mt.type"
               << " AND me.message_time < " << startTime
               << " AND me.message_time + mv.validityhours > " << startTime
               << " GROUP BY me.station_id,mt.type)";

    return withClause.str();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build from, where and order by clause with given station id's, message types,
 *        tables and time instant/range for querying accepted messages
 */
// ----------------------------------------------------------------------

void buildMessageQueryFromWhereOrderByClause(int maxMessageRows,
                                             const StationIdList& stationIdList,
                                             const QueryOptions& queryOptions,
                                             const TableMap& tableMap,
                                             const Config& config,
                                             const Column* timeRangeColumn,
                                             bool distinct,
                                             ostringstream& fromWhereOrderByClause)
{
  try
  {
    // 'record_set' or avidb_messages table is always selected by the query
    //
    // FROM { record_set | avidb_messages } me[,avidb_message_types mt][,avidb_message_routes
    // mr][,latest_messages lm][,message_validity mv][,avidb_stations st][,request_stations rs]
    //
    // Because 'message_validity' contains data only for message types queried with
    // MessageValidTimeRange[Latest] and MessageTimeRange[Latest] time restriction, it needs to be
    // outer joined with avidb_message_types in the main query for querying messages for other time
    // restriction types (when leftOuter is set for message_validity)

    const auto& knownMessageTypes = config.getMessageTypes();
    auto it = tableMap.find(messageValidityTableName);
    Table validityTable((it != tableMap.end()) ? it->second : Table());
    size_t n = 0;

    for (auto const& from : tableMap)
    {
      // No FROM clause entry for subquerys (latest_messages)
      //
      if (!from.second.subQuery)
      {
        if ((from.first == recordSetTableName) || (from.first == messageTableName) ||
            (((from.first != messageTypeTableName) || (!validityTable.leftOuter)) &&
             (!from.second.leftOuter) && (!from.second.itsJoin.empty())))
        {
          fromWhereOrderByClause << ((n == 0) ? " FROM " : ",") << from.first << " "
                                 << from.second.itsAlias;
          n++;
        }
        else if ((from.first == messageTypeTableName) && (!from.second.itsJoin.empty()))
        {
          fromWhereOrderByClause << ((n == 0) ? " FROM " : ",") << from.first << " "
                                 << from.second.itsAlias << " LEFT OUTER JOIN "
                                 << messageValidityTableName << " " << validityTable.itsAlias
                                 << " ON (" << validityTable.itsJoin << ")";
          n++;
        }
      }
    }

    // [ WHERE|AND me.route_id = mr.route_id ]
    // [ WHERE|AND me.type_id = mt.type_id ]
    // [ WHERE|AND me.message_id IN (SELECT message_id FROM latest_messages) ]
    // [ WHERE|AND mv.type = mt.type ]
    // [ WHERE|AND st.station_id = me.station_id ]
    // [ WHERE|AND me.station_id = rs.station_id ]

    const char* whereOrAnd = " WHERE ";

    for (auto const& where : tableMap)
    {
      if ((!where.second.leftOuter) && (!where.second.itsJoin.empty()))
      {
        fromWhereOrderByClause << whereOrAnd << where.second.itsJoin;
        whereOrAnd = " AND ";
      }
    }

    if (queryOptions.itsTimeOptions.itsObservationTime.empty())
    {
      if (queryOptions.itsTimeOptions.itsQueryValidRangeMessages)
      {
        // Querying valid messages within time range.
        //
        // Note: User given time range is taken as a half open range where start <= time < end. The
        // valid_from-valid_to range and any range calculated with range start time and period
        // length (here me.message_time + mv.validityhours) are taken as a closed range.
        //
        // AND
        // (
        //  [ (mt.type IN (ValidTimeRangeTypes,ValidTimeRangeLatestTypes) AND starttime <=
        //  me.valid_to
        //  AND endtime > me.valid_from) ]
        //  [ [ OR ] (mt.type IN (MessageValidTimeRangeTypes,MessageValidTimeRangeLatestTypes) AND (
        //			  (starttime <= me.valid_to AND endtime > me.message_time) OR
        //			  (me.valid_from IS NULL AND me.valid_to IS NULL AND (starttime <=
        // me.message_time + mv.validityhours AND endtime > me.message_time))
        //			 ))
        //	]
        //  [ [ OR ] (mt.type IN (MessageTimeRangeTypes) AND (starttime <= me.message_time +
        //  mv.validityhours AND endtime > me.message_time)) ]
        //  [ [ OR ] ([mt.type IN (MessageTimeRangeLatestTypes) AND] (me.message_id IN (SELECT
        //  message_id FROM messagetimerangelatest_messages))) ]
        //  [ [ OR ] (mt.type IN (CreationValidTimeRangeTypes,CreationValidTimeRangeLatestTypes) AND
        // (starttime <= me.valid_to AND endtime > me.creation_time)) ]
        // )
        //
        fromWhereOrderByClause << whereOrAnd << "(";

        list<TimeRangeType> timeRangeTypes;
        timeRangeTypes.push_back(TimeRangeType::ValidTimeRange);
        timeRangeTypes.push_back(TimeRangeType::ValidTimeRangeLatest);

        string messageTypeIn = buildMessageTypeInClause(
            queryOptions.itsMessageTypes, knownMessageTypes, timeRangeTypes);
        string emptyOrOr = "";

        if (!messageTypeIn.empty())
        {
          fromWhereOrderByClause << "(" << messageTypeIn << " AND "
                                 << "(" << queryOptions.itsTimeOptions.itsStartTime << " < "
                                 << messageTableAlias << ".valid_to"
                                 << " AND " << queryOptions.itsTimeOptions.itsEndTime << " > "
                                 << messageTableAlias << ".valid_from))";
          emptyOrOr = " OR ";
        }

        timeRangeTypes.clear();
        timeRangeTypes.push_back(TimeRangeType::MessageValidTimeRange);
        timeRangeTypes.push_back(TimeRangeType::MessageValidTimeRangeLatest);

        messageTypeIn = buildMessageTypeInClause(
            queryOptions.itsMessageTypes, knownMessageTypes, timeRangeTypes);

        if (!messageTypeIn.empty())
        {
          // BRAINSTORM-3301
          //
          // No (effective) query time restriction for edr -queries
          //
          bool disableQueryRestriction = (!queryOptions.itsTimeOptions.itsMessageTimeChecks);

          bool bStationJoin;
          string stationAndTimeRangeCondition =
              buildMessageValidTimeRangeTimeCondition(queryOptions.itsMessageTypes,
                                                      knownMessageTypes,
                                                      queryOptions.itsTimeOptions.itsStartTime,
                                                      queryOptions.itsTimeOptions.itsEndTime,
                                                      disableQueryRestriction,
                                                      bStationJoin);

          fromWhereOrderByClause << emptyOrOr << "(" << messageTypeIn << " AND ("
                                 << stationAndTimeRangeCondition << "))";
          emptyOrOr = " OR ";
        }

        messageTypeIn = buildMessageTypeInClause(
            queryOptions.itsMessageTypes, knownMessageTypes, TimeRangeType::MessageTimeRange);

        if (!messageTypeIn.empty())
        {
          fromWhereOrderByClause << emptyOrOr << "(" << messageTypeIn << " AND "
                                 << "(" << queryOptions.itsTimeOptions.itsStartTime << " < ("
                                 << messageTableAlias << ".message_time + "
                                 << messageValidityTableAlias << ".validityhours)"
                                 << " AND " << queryOptions.itsTimeOptions.itsEndTime << " > "
                                 << messageTableAlias << ".message_time))";
          emptyOrOr = " OR ";
        }

        // For MessageTimeRangeLatest restriction querying 'messagetimerangelatest_messages' for the
        // id's of the latest valid messages having message_time earlier than starttime in addition
        // to all messages having starttime <= message_time < endtime
        //
        // Note: Even through 'messagetimerangelatest_messages' contains message id's only for
        // MessageTimeRangeLatest types, type restriction must be combined with IN clause below;
        // otherwise query throughput drastically drops with longer time ranges (query planner is
        // somehow fooled by the query)

        messageTypeIn = buildMessageTypeInClause(
            queryOptions.itsMessageTypes, knownMessageTypes, TimeRangeType::MessageTimeRangeLatest);

        if (!messageTypeIn.empty())
        {
          fromWhereOrderByClause << emptyOrOr << "(" << messageTypeIn << " AND ("
                                 << messageTableAlias << ".message_id IN (SELECT message_id FROM "
                                 << messageTimeRangeLatestMessagesTableName << ")))";
          emptyOrOr = " OR ";
        }

        timeRangeTypes.clear();
        timeRangeTypes.push_back(TimeRangeType::CreationValidTimeRange);
        timeRangeTypes.push_back(TimeRangeType::CreationValidTimeRangeLatest);

        messageTypeIn = buildMessageTypeInClause(
            queryOptions.itsMessageTypes, knownMessageTypes, timeRangeTypes);

        if (!messageTypeIn.empty())
          fromWhereOrderByClause << emptyOrOr << "(" << messageTypeIn << " AND "
                                 << "(" << queryOptions.itsTimeOptions.itsStartTime << " < "
                                 << messageTableAlias << ".valid_to"
                                 << " AND " << queryOptions.itsTimeOptions.itsEndTime << " > "
                                 << messageTableAlias << ".created))";

        fromWhereOrderByClause << ")";
      }
      else
      {
        // Querying messages created within time range.
        //
        // Note: User given time range is taken as a half open range where start <= time < end
        //
        // AND me.statation_id IN (StationIdList)
        // AND mt.type IN (MessageTypeList) ]
        // AND me.message_time >= starttime AND me.message_time < endtime
        //
        if (!timeRangeColumn)
          throw Fmi::Exception(
              BCP, "buildMessageQueryFromWhereOrderByClause(): internal: time column is NULL");

        buildMessageQueryWhereStationIdInClause(stationIdList, fromWhereOrderByClause);
        string messageTypeIn = buildMessageTypeInClause(
            queryOptions.itsMessageTypes, knownMessageTypes, list<TimeRangeType>());

        fromWhereOrderByClause << " AND " << messageTypeIn << " AND " << messageTableAlias << "."
                               << timeRangeColumn->getTableColumnName()
                               << " >= " << queryOptions.itsTimeOptions.itsStartTime << " AND "
                               << messageTableAlias << "." << timeRangeColumn->getTableColumnName()
                               << " < " << queryOptions.itsTimeOptions.itsEndTime;

        bool filterMETARs = (queryOptions.itsFilterMETARs && config.getFilterFIMETARxxx() &&
                             (messageTypeIn.find("'METAR") != string::npos));
        bool excludeSPECIs =
            (queryOptions.itsExcludeSPECIs && (messageTypeIn.find("'SPECI'") != string::npos));

        if (filterMETARs || excludeSPECIs)
        {
          // AND (
          //      st.country_code != 'FI' OR
          //      (
          //       [
          //       (
          //        (mt.type != 'METAR' OR me.message LIKE 'METAR%')
          //        [ OR st.icao_code IN (ExcludedIcaoList) ]
          //       )
          //       ]
          //       [ [ AND ] mt.type != 'SPECI' ]
          //      )
          //     )

          fromWhereOrderByClause << " AND (" << stationTableAlias << ".country_code != 'FI' OR (";

          if (filterMETARs)
          {
            fromWhereOrderByClause << "((" << messageTypeTableAlias << ".type != 'METAR' OR "
                                   << messageTableAlias << ".message LIKE 'METAR%')";

            auto const& filterFIMETARxxxExcludeIcaos = config.getFilterFIMETARxxxExcludeIcaos();

            if (!filterFIMETARxxxExcludeIcaos.empty())
              fromWhereOrderByClause << " OR st.icao_code IN ("
                                     << getStringList(filterFIMETARxxxExcludeIcaos) << ")";

            fromWhereOrderByClause << ")";
          }

          if (excludeSPECIs)
            fromWhereOrderByClause << (filterMETARs ? " AND " : "") << messageTypeTableAlias
                                   << ".type != 'SPECI'";

          fromWhereOrderByClause << "))";
        }
      }
    }
    else
    {
      // Message restriction made by join to latest_messages.message_id
    }

    if (!queryOptions.itsLocationOptions.itsBBoxes.empty())
    {
      // BRAINSTORM-3136; when using bbox(es), message query now filters stations with
      // bbox(es)/maxdistance, not with preselected station id list
      //
      // Avi plugin was also modified not to allow use of multiple location options (it has
      // been the configured case anyway), it was the simpliest way to handle the change

      ostringstream whereStationClause;

      buildStationQueryWhereClause(queryOptions.itsLocationOptions.itsBBoxes,
                                   queryOptions.itsLocationOptions.itsMaxDistance,
                                   whereStationClause,
                                   "",
                                   stationTableAlias);

      fromWhereOrderByClause << " AND (" << whereStationClause.str() << ")";
    }

    // ORDER BY { st.icao_code | rs.position } [,me.message] [,me.message_id]

    if (!queryOptions.itsLocationOptions.itsWKTs.isRoute)
      fromWhereOrderByClause << " ORDER BY " << stationTableAlias << "." << stationIcaoTableColumn;
    else if (stationIdList.empty())
      fromWhereOrderByClause << " ORDER BY " << messageTableAlias << "."
                             << messageStationIdTableColumn;
    else
      fromWhereOrderByClause << " ORDER BY " << requestStationsTableAlias << "."
                             << requestStationsPositionColumn;

    if (!distinct)
    {
      if (queryOptions.itsDistinctMessages)
        // Using message for ordering too (needed to check/skip duplicates)
        //
        fromWhereOrderByClause << "," << messageTableAlias << "." << messageTableColumn
                               << " COLLATE \"C\"";

      // Using message id for ordering too (needed to ensure regression tests can succeed)

      fromWhereOrderByClause << "," << messageTableAlias << "." << messageIdTableColumn;
    }

    // [ LIMIT maxMessageRows ]

    if (maxMessageRows > 0)
      fromWhereOrderByClause << " LIMIT " << maxMessageRows + 1;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build from, where and order by clause with given message types,
 *        message tables and time range for querying rejected messages
 */
// ----------------------------------------------------------------------

void buildRejectedMessageQueryFromWhereOrderByClause(int maxMessageRows,
                                                     const QueryOptions& queryOptions,
                                                     const TableMap& tableMap,
                                                     ostringstream& fromWhereOrderByClause)
{
  try
  {
    // 'avidb_rejected_messages' table is always joined into the query
    //
    // Join to 'avidb_message_routes' is always LEFT JOIN.
    // Join to 'avidb_message_types' is LEFT JOIN (no message type restriction) or by WHERE
    // condition
    // (with message type restriction)
    //
    // FROM avidb_rejected_messages me
    //		[LEFT JOIN avidb_message_routes mr ON (me.route_id = mr.route_id) ]
    //		[LEFT JOIN avidb_message_types mt ON (me.type_id = mt.type_id) |
    //,avidb_message_types
    // mt
    // WHERE me.type_id = mt.type_id ]

    fromWhereOrderByClause << " FROM " << rejectedMessageTableName << " "
                           << rejectedMessageTableAlias;

    // First generate LEFT JOIN(s) if any

    for (auto const& join : tableMap)
      if (join.second.leftOuter && (!join.second.itsJoin.empty()))
        fromWhereOrderByClause << " LEFT JOIN " << join.first << " " << join.second.itsAlias
                               << " ON " << join.second.itsJoin;

    // Then other FROM tables (avidb_message_types) if any

    for (auto const& from : tableMap)
      if ((from.first != rejectedMessageTableName) && (!from.second.leftOuter) &&
          ((!from.second.itsSelectedColumns.empty()) || (!from.second.itsJoin.empty())))
        fromWhereOrderByClause << "," << from.first << " " << from.second.itsAlias;

    // Then WHERE join condition(s) if any;
    //
    // [ WHERE me.type_id = mt.type_id ]

    size_t n = 0;

    for (auto const& where : tableMap)
    {
      if ((!where.second.leftOuter) && (!where.second.itsJoin.empty()))
      {
        fromWhereOrderByClause << ((n == 0) ? " WHERE " : " AND ") << where.second.itsJoin;
        n++;
      }
    }

    // [ WHERE|AND mt.type IN (messageTypeList) ]

    string whereOrAnd((n == 0) ? " WHERE " : " AND ");

    if (!queryOptions.itsMessageTypes.empty())
    {
      string messageTypeIn = whereOrAnd + "UPPER(" + messageTypeTableAlias + ".type) IN ('";
      n = 0;

      for (auto const& messageType : queryOptions.itsMessageTypes)
      {
        fromWhereOrderByClause << ((n == 0) ? messageTypeIn : "','")
                               << Fmi::ascii_toupper_copy(messageType);
        n++;
      }

      fromWhereOrderByClause << "')";

      whereOrAnd = " AND ";
    }

    // AND (me.created >= starttime AND me.created < endtime)

    fromWhereOrderByClause << whereOrAnd << "(" << rejectedMessageTableAlias
                           << ".created >= " << queryOptions.itsTimeOptions.itsStartTime << " AND "
                           << rejectedMessageTableAlias << ".created < "
                           << queryOptions.itsTimeOptions.itsEndTime << ")";

    // ORDER BY me.icao_code,me.created

    fromWhereOrderByClause << " ORDER BY " << rejectedMessageTableAlias << "."
                           << rejectedMessageIcaoTableColumn << "," << rejectedMessageTableAlias
                           << ".created";

    // [ LIMIT maxMessageRows ]

    if (maxMessageRows > 0)
      fromWhereOrderByClause << " LIMIT " << maxMessageRows + 1;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Sort columns by their position in the 'param=' list of the request
 */
// ----------------------------------------------------------------------

void sortColumnList(Columns& columns)
{
  try
  {
    columns.sort(columns.front().columnNumberSort);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // anonymous namespace

// ----------------------------------------------------------------------
/*!
 * \brief The only permitted constructor requires a configfile
 */
// ----------------------------------------------------------------------

EngineImpl::EngineImpl(std::string theConfigFileName)
    : itsConfigFileName(std::move(theConfigFileName))
{
}

// ----------------------------------------------------------------------
/*!
 * \brief Initialize the engine
 *
 * Note: We do not wish to delay the initialization of other engines.
 * init() is done in its own thread, hence we read the configuration
 * files here, and hence itsConfig is a pointer instead of an object.
 */
// ----------------------------------------------------------------------

void EngineImpl::init()
{
  try
  {
    itsConfig = std::make_shared<Config>(itsConfigFileName);

    itsConnectionPool = std::make_unique<Fmi::Database::PostgreSQLConnectionPool>(
        itsConfig->getStartConnections(),
        itsConfig->getMaxConnections(),
        mk_connection_options(*itsConfig));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Init failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Shutdown the engine
 */
// ----------------------------------------------------------------------

void EngineImpl::shutdown()
{
  std::cout << "  -- Shutdown requested (aviengine)\n";
}

// ----------------------------------------------------------------------
/*!
 * \brief Get column mapping using query column name
 */
// ----------------------------------------------------------------------

const Column* EngineImpl::getQueryColumn(const ColumnTable& tableColumns,
                                         Columns& columns,
                                         const string& theQueryColumnName,
                                         bool& duplicate,
                                         int columnNumber) const
{
  try
  {
    // Column can be selected only once.
    //
    // Note: stationid is and distance can be automatically selected; if they are
    //       requested by the caller, set request column number

    duplicate = false;

    for (const Column* column = tableColumns; (!(column->itsName.empty())); column++)
    {
      if (column->itsName == theQueryColumnName)
      {
        for (auto& column : columns)
          if (column.itsName == theQueryColumnName)
          {
            if (column.itsSelection == ColumnSelection::Automatic)
            {
              column.itsSelection = ColumnSelection::AutomaticRequested;
              column.itsNumber = columnNumber;
            }

            duplicate = true;

            return nullptr;
          }

        return column;
      }
    }
    return nullptr;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build select expressions for coordinate dependent derived fields
 */
// ----------------------------------------------------------------------

string EngineImpl::buildStationQueryCoordinateExpressions(const Columns& columns) const
{
  try
  {
    // ST_Distance(geom::geography,ST_SetSRID(coordinates.coordinate,4326)::geography) AS distance
    // DEGREES(ST_Azimuth(geom,ST_SetSRID(coordinates.coordinate,4326))) AS bearing

    ostringstream selectExpressions;

    for (auto const& column : columns)
      if (column.itsCoordinateExpression)
        selectExpressions << ","
                          << column.itsCoordinateExpression(column.itsTableColumnName,
                                                            column.itsName);

    return selectExpressions.str();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build select clause for querying stations
 */
// ----------------------------------------------------------------------

Columns EngineImpl::buildStationQuerySelectClause(const StringList& paramList,
                                                  bool selectStationListOnly,
                                                  bool autoSelectDistance,
                                                  string& selectClause,
                                                  bool& firIdQuery) const
{
  try
  {
    if (paramList.empty())
      throw Fmi::Exception(BCP, "No parameters were given to station query");

    // Selected columns

    Columns columns;
    bool duplicate;

    selectClause.clear();
    firIdQuery = false;

    // Station id is automatically selected

    auto queryColumn =
        getQueryColumn(stationQueryColumns, columns, stationIdQueryColumn, duplicate);

    if (!queryColumn)
      throw Fmi::Exception(BCP,
                           "buildStationQuerySelectClause(): internal: Unable to get id column");

    Column column(*queryColumn);
    column.itsSelection = ColumnSelection::Automatic;
    columns.push_back(column);

    selectClause =
        string("SELECT ") + queryColumn->itsTableColumnName + " AS " + queryColumn->itsName;

    // distance is automatically selected to apply max # of nearest stations

    if (autoSelectDistance)
    {
      auto queryColumn =
          getQueryColumn(stationQueryColumns, columns, stationDistanceQueryColumn, duplicate);

      if (!queryColumn)
        throw Fmi::Exception(
            BCP, "buildStationQuerySelectClause(): internal: Unable to get distance column");

      Column column(*queryColumn);
      column.itsSelection = ColumnSelection::Automatic;
      columns.push_back(column);
    }

    int columnNumber = 0;

    for (auto const& param : paramList)
    {
      // Note: If 'selectStationList' is set, scanning only for derived distance and bearing columns
      // (they are not available in current message query), and setting 'AutomaticRequested'
      // (instead of 'Automatic') type for the user requested fields (stationid) to indicate they
      // shall be returned
      //
      auto queryColumn =
          getQueryColumn(stationQueryColumns, columns, param, duplicate, columnNumber);

      if (queryColumn && ((!selectStationListOnly) || queryColumn->itsCoordinateExpression))
      {
        if (queryColumn->itsExpression)
        {
          // ST_X(geom) AS longitudes
          // ST_Y(geom) AS latitude
          // ST_X(geom) || ',' || ST_Y(geom) AS lonlat
          // ST_Y(geom) || ',' || ST_X(geom) AS latlon
          //
          selectClause +=
              (string(selectClause.empty() ? "SELECT " : ",") +
               queryColumn->itsExpression(queryColumn->itsTableColumnName, queryColumn->itsName));
        }
        else if (queryColumn->itsCoordinateExpression)
        {
          // Parametrized expression for each given coordinate
          //
          // Note: Column expression is generated and added to the select clause only when querying
          // with coordinates (queryStationsWithCoordinates())
        }
        else
        {
          selectClause +=
              (string(selectClause.empty() ? "SELECT " : ",") + queryColumn->itsTableColumnName);

          if (queryColumn->itsType == ColumnType::DateTime)
            selectClause += string(" AT TIME ZONE 'UTC'");

          if ((queryColumn->itsType == ColumnType::DateTime) ||
              (queryColumn->itsName != queryColumn->itsTableColumnName))
            selectClause += (string(" AS ") + queryColumn->itsName);
        }

        columns.push_back(*queryColumn);
        columns.back().itsNumber = columnNumber;
      }
      else if ((!queryColumn) && (!duplicate) && (!selectStationListOnly))
      {
        queryColumn = getQueryColumn(firQueryColumns, columns, param, duplicate, columnNumber);

        if (queryColumn)
        {
          firIdQuery = true;
          selectClause += (string(",") + firTableAlias + "." + firIdTableColumn + " AS " +
                           queryColumn->itsName);

          columns.push_back(*queryColumn);
          columns.back().itsNumber = columnNumber;
        }
      }

      columnNumber++;
    }

    return columns;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build select clause for querying accepted or rejected messages
 */
// ----------------------------------------------------------------------

TableMap EngineImpl::buildMessageQuerySelectClause(QueryTable* queryTables,
                                                   const StationIdList& stationIdList,
                                                   const StringList& messageTypeList,
                                                   const StringList& paramList,
                                                   bool routeQuery,
                                                   string& selectClause,
                                                   bool& messageColumnSelected,
                                                   bool& distinct) const
{
  try
  {
    // Selected tables/columns

    TableMap tableMap;
    bool leftOuter = true, icaoSelected = false, duplicate;
    bool distinctMessages = distinct, checkDuplicateMessages = false;
    int columnNumber = 0;

    selectClause.clear();
    messageColumnSelected = distinct = false;

    // For accepted messages, station id is automatically selected for storing the data (station id
    // is data map key) and for joining with station data (to get distance and bearing), and for non
    // route query icao_code column is added to the list of query columns to join with station table
    // for ordering the messages by icao code.
    //
    // Message column is automatically selected to check duplicates if any other message table
    // column is selected

    if (queryTables->itsName == messageTableName)
    {
      // Because queries like "param=name,messagetype,route" - selecting columns from
      // avidb_stations, avidb_message_types and avidb_message_routes, but none from
      // avidb_messages - are allowed, the select for accepted messages is distinct unless at
      // least one avidb_messages column other than station id is selected
      //
      // No LEFT OUTER joins when querying accepted messages
      //
      distinct = true;
      leftOuter = false;

      for (QueryTable* qt = queryTables;; qt++)
      {
        const QueryTable& queryTable = *qt;

        if (queryTable.itsName.empty())
          break;

        if (queryTable.itsName == messageTableName)
        {
          auto& table = tableMap[messageTableName];
          table.itsAlias = queryTable.itsAlias;
          table.itsJoin = queryTable.itsJoin;

          auto queryColumn = getQueryColumn(
              queryTable.itsColumns, table.itsSelectedColumns, stationIdQueryColumn, duplicate);

          if (!queryColumn)
            throw Fmi::Exception(
                BCP, "buildMessageQuerySelectClause(): internal: Unable to get station id column");

          Column column(*queryColumn);
          column.itsSelection = ColumnSelection::Automatic;
          table.itsSelectedColumns.push_back(column);

          selectClause = queryTable.itsAlias + "." + queryColumn->itsTableColumnName + " AS " +
                         queryColumn->itsName;
        }
        else if ((!routeQuery) && (queryTable.itsName == stationTableName))
        {
          auto& table = tableMap[stationTableName];
          table.itsAlias = queryTable.itsAlias;
          table.itsJoin = queryTable.itsJoin;

          auto queryColumn = getQueryColumn(
              queryTable.itsColumns, table.itsSelectedColumns, stationIcaoQueryColumn, duplicate);

          if (!queryColumn)
            throw Fmi::Exception(
                BCP,
                "buildMessageQuerySelectClause(): internal: Unable to get station icao column");

          Column column(*queryColumn);
          column.itsSelection = ColumnSelection::Automatic;
          table.itsSelectedColumns.push_back(column);

          // NOTE: Generate select expression if icao is requested by the caller too. When looping
          // the parameters below, column's select expression is NOT generated if the column already
          // exists in column list; it's type just gets changed to AutomaticRequested

          if (find(paramList.begin(), paramList.end(), stationIcaoQueryColumn) != paramList.end())
          {
            selectClause += (string(selectClause.empty() ? "" : ",") + queryTable.itsAlias + "." +
                             queryColumn->itsTableColumnName + " AS " + queryColumn->itsName);
            icaoSelected = true;
          }
        }
      }
    }

    for (auto const& param : paramList)
    {
      for (QueryTable* qt = queryTables;; qt++)
      {
        const QueryTable& queryTable = *qt;

        if (queryTable.itsName.empty())
          break;

        auto& table = tableMap[queryTable.itsName];
        auto queryColumn = getQueryColumn(
            queryTable.itsColumns, table.itsSelectedColumns, param, duplicate, columnNumber);

        if (queryColumn)
        {
          if (table.itsAlias.empty())
            table.itsAlias = queryTable.itsAlias;

          if (table.itsJoin.empty())
            table.itsJoin = queryTable.itsJoin;

          table.leftOuter = leftOuter;

          if (queryColumn->itsExpression)
          {
            selectClause +=
                (string(selectClause.empty() ? "" : ",") +
                 queryColumn->itsExpression(queryColumn->itsTableColumnName, queryColumn->itsName));
          }
          else if (queryColumn->itsCoordinateExpression)
          {
            // Select NULL for distance and bearing columns; they will be set after the query by
            // searching from station data
            //
            selectClause += (string(selectClause.empty() ? "" : ",") + nullExpression(queryColumn));
          }
          else
          {
            selectClause += (string(selectClause.empty() ? "" : ",") + queryTable.itsAlias + "." +
                             queryColumn->itsTableColumnName);

            if (queryColumn->itsType == ColumnType::DateTime)
              selectClause += string(" AT TIME ZONE 'UTC'");

            if ((queryColumn->itsType == ColumnType::DateTime) ||
                (queryColumn->itsName != queryColumn->itsTableColumnName))
              selectClause += (string(" AS ") + queryColumn->itsName);
          }

          if ((queryTable.itsName != stationTableName) && (queryTable.itsName != firTableName))
          {
            if (queryTable.itsName == messageTableName)
            {
              distinct = false;

              if (distinctMessages && (queryColumn->itsName == messageQueryColumn))
                // Clear flag forcing automatic 'message' column selection
                //
                distinctMessages = false;
              else
                checkDuplicateMessages = true;
            }

            messageColumnSelected = true;
          }

          table.itsSelectedColumns.push_back(*queryColumn);
          table.itsSelectedColumns.back().itsNumber = columnNumber;

          break;
        }
        else if (duplicate)
          // Automatically selected column is selected by the caller too
          //
          break;
      }

      columnNumber++;
    }

    if (distinctMessages && checkDuplicateMessages)
    {
      // 'message' column was not selected by the caller; select it to skip duplicates
      //
      auto& table = tableMap[messageTableName];
      auto queryColumn = getQueryColumn(
          messageQueryColumns, table.itsSelectedColumns, messageQueryColumn, duplicate);

      if (!queryColumn)
        throw Fmi::Exception(
            BCP, "buildMessageQuerySelectClause(): internal: Unable to get message column");

      Column column(*queryColumn);
      column.itsSelection = ColumnSelection::Automatic;
      table.itsSelectedColumns.push_back(column);

      selectClause += (string(",") + messageTableAlias + "." + queryColumn->itsTableColumnName +
                       " AS " + queryColumn->itsName);
    }

    // SELECT [DISTINCT] ...
    //
    // Note: "for SELECT DISTINCT, ORDER BY expressions must appear in select list"; ensure
    // avidb_stations is joined and icao is selected for non route query, or select 'position'
    // column for route query

    selectClause = (distinct ? "SELECT DISTINCT " : "SELECT ") + selectClause;

    if ((!routeQuery) && distinct && (!icaoSelected))
    {
      auto& table = tableMap[stationTableName];

      if (table.itsAlias.empty())
        table.itsAlias = stationTableAlias;

      if (table.itsJoin.empty())
        table.itsJoin = stationTableJoin;

      selectClause += (string(",") + stationTableAlias + "." + stationIcaoTableColumn);
    }
    else if (routeQuery && distinct && (!stationIdList.empty()))
      selectClause +=
          (string(",") + requestStationsTableAlias + "." + requestStationsPositionColumn);

    // Ensure 'avidb_message_types' table is joined into the query if restricting query with given
    // message types

    if (messageColumnSelected && (!messageTypeList.empty()))
    {
      auto& table = tableMap[messageTypeTableName];

      if (table.itsAlias.empty())
        table.itsAlias = messageTypeTableAlias;

      if (table.itsJoin.empty())
        table.itsJoin = messageTypeTableJoin;

      table.leftOuter = false;
    }

    return tableMap;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Load query result into given data object
 */
// ----------------------------------------------------------------------

template <typename T>
void EngineImpl::loadQueryResult(
    const pqxx::result& result, bool debug, T& queryData, bool distinctRows, int maxRows) const
{
  try
  {
    if (debug)
      cerr << "Rows: " << result.size() << '\n';

    if ((maxRows > 0) && ((int)result.size() > maxRows))
      throw Fmi::Exception(
          BCP,
          string("Max number of rows exceeded (") + Fmi::to_string(maxRows) + "), limit the query");

    bool checkDuplicateMessages =
        (distinctRows &&
         (find(queryData.itsColumns.begin(), queryData.itsColumns.end(), messageQueryColumn) !=
          queryData.itsColumns.end()));
    bool duplicate;

    for (pqxx::result::const_iterator row = result.begin(), prevRow = result.begin();
         (row != result.end());
         row++)
    {
      QueryValues& queryValues =
          queryData.getValues(row, checkDuplicateMessages ? prevRow : row, duplicate);

      if (duplicate && distinctRows)
        // Station or message was already selected
        //
        continue;

      prevRow = row;

      for (const Column& column : queryData.itsColumns)
      {
        // For station data (stations and accepted messages) automatically selected station id is
        // stored as a map key; it is not stored as a column if it was not requested.
        //
        // Also distance is selected automatically for stations to apply max # of nearest stations,
        // and icao is automatically added to the column list to generate station table join to
        // order the messages by icao code
        //
        if (column.itsSelection == ColumnSelection::Automatic)
          // Column was not requested by the caller, skip it
          //
          continue;

        if (column.itsType == ColumnType::Integer)
        {
          // Currently can't handle NULLs properly, but by setting kFloatMissing for NULL,
          // TableFeeder (used by avi plugin) produces 'missing' (by default, 'nan') column value.
          //
          // Solution is poor, numeric column value 32700 cannot be returned by avi plugin
          // (in practice through, only stationid or messageid could have value 32700).
          //
          TimeSeries::Value return_value = TimeSeries::None();
          if (!row[column.itsName].is_null())
            return_value = row[column.itsName].as<int>();

          queryValues[column.itsName].push_back(return_value);
        }
        else if (column.itsType == ColumnType::Double)
        {
          // Note: try/catch; With station query distance and bearing are available only when
          // querying stations with coordinates; for other station queries distance and bearing
          // are not selected at all
          //
          TimeSeries::Value return_value = TimeSeries::None();
          try
          {
            if (!row[column.itsName].is_null())
              return_value = row[column.itsName].as<double>();
          }
          catch (...)
          {
            return_value = TimeSeries::None();
          }

          queryValues[column.itsName].push_back(return_value);
        }
        else if (column.itsType == ColumnType::String)
        {
          bool isNull;

          try
          {
            isNull = row[column.itsName].is_null();
          }
          catch (...)
          {
            isNull = true;
          }

          if (isNull)
            queryValues[column.itsName].emplace_back(TimeSeries::None());
          else
            queryValues[column.itsName].emplace_back(
                boost::algorithm::trim_copy(row[column.itsName].as<string>()));
        }
        else if ((column.itsType == ColumnType::TS_LonLat) ||
                 (column.itsType == ColumnType::TS_LatLon))
        {
          // 'latlon' and 'lonlat' are selected as comma separated strings. Return them as
          // TimeSeries::LonLat for formatted output with TableFeeder
          //
          TimeSeries::LonLat lonlat(0, 0);
          string llStr(boost::algorithm::trim_copy(row[column.itsName].as<string>()));
          vector<string> flds;
          boost::algorithm::split(flds, llStr, boost::is_any_of(","));
          bool lonlatValid = false;

          if (flds.size() == 2)
          {
            string& lon = ((column.itsName == stationLonLatQueryColumn) ? flds[0] : flds[1]);
            string& lat = ((column.itsName == stationLonLatQueryColumn) ? flds[1] : flds[0]);

            boost::algorithm::trim(lon);
            boost::algorithm::trim(lat);

            try
            {
              lonlat.lon = Fmi::stod(lon);
              lonlat.lat = Fmi::stod(lat);
              lonlatValid = true;
            }
            catch (...)
            {
            }
          }

          if (!lonlatValid)
            throw Fmi::Exception(
                BCP, string("Query returned invalid ") + column.itsName + " value '" + llStr + "'");

          queryValues[column.itsName].emplace_back(lonlat);
        }
        else
        {
          Fmi::LocalDateTime utcTime(
              row[column.itsName].is_null()
                  ? Fmi::DateTime()
                  : Fmi::DateTime::from_string(row[column.itsName].as<string>()),
              tzUTC);
          queryValues[column.itsName].emplace_back(utcTime);
        }
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Execute query and load the result into given data object
 */
// ----------------------------------------------------------------------

template <typename T>
void EngineImpl::executeQuery(const Fmi::Database::PostgreSQLConnection& connection,
                              const string& query,
                              bool debug,
                              T& queryData,
                              bool distinctRows,
                              int maxRows) const
{
  try
  {
    if (debug)
      cerr << "Query: " << query << '\n';

    auto result = connection.executeNonTransaction(query);

    loadQueryResult(result, debug, queryData, distinctRows, maxRows);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Execute parametrized query and load query result into given data object
 */
// ----------------------------------------------------------------------

template <typename T>
void EngineImpl::executeParamQuery(const Fmi::Database::PostgreSQLConnection& connection,
                                   const string& query,
                                   const string& queryArg,
                                   bool debug,
                                   T& queryData,
                                   bool distinctRows,
                                   int maxRows) const
{
  try
  {
    if (debug)
      cerr << "Query: " << query << '\n';

    auto result = connection.exec_params(query, queryArg);

    loadQueryResult(result, debug, queryData, distinctRows, maxRows);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

template <typename T, typename T2>
void EngineImpl::executeParamQuery(const Fmi::Database::PostgreSQLConnection& connection,
                                   const string& query,
                                   const T2& queryArgs,
                                   bool debug,
                                   T& queryData,
                                   bool distinctRows,
                                   int maxRows) const
{
  try
  {
    if (debug)
      cerr << "Query: " << query << '\n';

    auto result = connection.exec_params_p(query, queryArgs);

    loadQueryResult(result, debug, queryData, distinctRows, maxRows);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations with given coordinates
 */
// ----------------------------------------------------------------------

void EngineImpl::queryStationsWithCoordinates(const Fmi::Database::PostgreSQLConnection& connection,
                                              const LocationOptions& locationOptions,
                                              const StringList& messageTypes,
                                              const string& selectClause,
                                              bool debug,
                                              StationQueryData& stationQueryData) const
{
  try
  {
    // Build select expressions for coordinate dependent derived columns

    string derivedColumnSelectExpressions =
        buildStationQueryCoordinateExpressions(stationQueryData.itsColumns);

    // Build from and where clause

    ostringstream fromWhereClause;

    buildStationQueryFromWhereClause(locationOptions, messageTypes, fromWhereClause);

    // Add 'SELECT FROM (...)' query level(s) to apply DISTINCT ON (station_id) and max # of nearest
    // stations

    ostringstream selectFromClause;

    selectFromClause << "SELECT DISTINCT ON (" + string(stationIdQueryColumn) + ") * FROM (";
    fromWhereClause << ") AS stations";

    if (locationOptions.itsNumberOfNearestStations > 0)
    {
      selectFromClause << "SELECT *,RANK() OVER (PARTITION BY " << stationCoordinateColumn
                       << " ORDER BY " << stationDistanceQueryColumn << ") AS rank FROM (";
      fromWhereClause << ") AS stations WHERE rank <= " << fixed
                      << locationOptions.itsNumberOfNearestStations;
    }

    // Execute query

    const string query = selectFromClause.str() + selectClause + derivedColumnSelectExpressions +
                         ",coordinates." + stationCoordinateColumn + " " + fromWhereClause.str();

    executeQuery<StationQueryData>(connection, query, debug, stationQueryData);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations with given station id's
 */
// ----------------------------------------------------------------------

void EngineImpl::queryStationsWithIds(const Fmi::Database::PostgreSQLConnection& connection,
                                      const StationIdList& stationIdList,
                                      const string& selectClause,
                                      bool debug,
                                      StationQueryData& stationQueryData) const
{
  try
  {
    // Build where clause and execute query

    ostringstream whereClause;

    buildStationQueryWhereClause(stationIdList, whereClause);

    executeQuery<StationQueryData>(connection,
                                   selectClause + " FROM avidb_stations " + whereClause.str(),
                                   debug,
                                   stationQueryData);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations with given icao codes
 */
// ----------------------------------------------------------------------

void EngineImpl::queryStationsWithIcaos(const Fmi::Database::PostgreSQLConnection& connection,
                                        const StringList& icaoList,
                                        const string& selectClause,
                                        bool firIdQuery,
                                        bool debug,
                                        StationQueryData& stationQueryData) const
{
  try
  {
    // Build where clause and execute query

    ostringstream whereClause;

    string fromClause(string(" FROM avidb_stations ") + stationTableAlias);

    buildStationQueryWhereClause(connection, "UPPER(icao_code)", icaoList, "", {}, whereClause);

    if (firIdQuery)
    {
      fromClause += (string(",") + firTableName + " AS " + firTableAlias);
      whereClause << " AND " << firTableJoin;
    }

    executeQuery<StationQueryData>(
        connection, selectClause + fromClause + " " + whereClause.str(), debug, stationQueryData);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations with given country codes
 */
// ----------------------------------------------------------------------

void EngineImpl::queryStationsWithCountries(const Fmi::Database::PostgreSQLConnection& connection,
                                            const StringList& countryList,
                                            const StringList& excludeIcaoFilters,
                                            const string& selectClause,
                                            bool firIdQuery,
                                            bool debug,
                                            StationQueryData& stationQueryData) const
{
  try
  {
    // Build where clause and execute query

    ostringstream whereClause;

    string fromClause(string(" FROM avidb_stations ") + stationTableAlias);

    buildStationQueryWhereClause(connection,
                                 "UPPER(country_code)",
                                 countryList,
                                 "icao_code",
                                 excludeIcaoFilters,
                                 whereClause);

    if (firIdQuery)
    {
      fromClause += (string(",") + firTableName + " AS " + firTableAlias);
      whereClause << " AND " << firTableJoin;
    }

    executeQuery<StationQueryData>(
        connection, selectClause + fromClause + " " + whereClause.str(), debug, stationQueryData);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations with given places (station names)
 */
// ----------------------------------------------------------------------

void EngineImpl::queryStationsWithPlaces(const Fmi::Database::PostgreSQLConnection& connection,
                                         const StringList& placeNameList,
                                         const string& selectClause,
                                         bool debug,
                                         StationQueryData& stationQueryData) const
{
  try
  {
    // Build where clause and execute query

    ostringstream whereClause;

    buildStationQueryWhereClause(
        connection, "UPPER(BTRIM(name))", placeNameList, "", {}, whereClause);

    executeQuery<StationQueryData>(connection,
                                   selectClause + " FROM avidb_stations " + whereClause.str(),
                                   debug,
                                   stationQueryData);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations with given wkts
 */
// ----------------------------------------------------------------------

void EngineImpl::queryStationsWithWKTs(const Fmi::Database::PostgreSQLConnection& connection,
                                       const LocationOptions& locationOptions,
                                       const StringList& messageTypes,
                                       const string& selectClause,
                                       bool debug,
                                       StationQueryData& stationQueryData) const
{
  try
  {
    // Build from and where (and order by for route query) clauses and execute query

    ostringstream fromWhereOrderByClause;

    buildStationQueryFromWhereOrderByClause(connection,
                                            locationOptions,
                                            messageTypes,
                                            itsConfig->getMessageTypes(),
                                            fromWhereOrderByClause);

    // Note: no stations (fromWhereOrderByClause is empty) to restrict messages for route query
    // for global scoped message types (message time and type restriction is generated later)

    if (!fromWhereOrderByClause.str().empty())
      executeQuery<StationQueryData>(
          connection, selectClause + fromWhereOrderByClause.str(), debug, stationQueryData);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations with given bboxes
 */
// ----------------------------------------------------------------------

void EngineImpl::queryStationsWithBBoxes(const Fmi::Database::PostgreSQLConnection& connection,
                                         const LocationOptions& locationOptions,
                                         const string& selectClause,
                                         bool debug,
                                         StationQueryData& stationQueryData) const
{
  try
  {
    // Build where clause and execute query

    ostringstream whereClause;

    buildStationQueryWhereClause(
        locationOptions.itsBBoxes, locationOptions.itsMaxDistance, whereClause);

    executeQuery<StationQueryData>(connection,
                                   selectClause + " FROM avidb_stations " + whereClause.str(),
                                   debug,
                                   stationQueryData);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Check time parameter values are valid
 */
// ----------------------------------------------------------------------

namespace
{

Fmi::DateTime parseTime(const string& timeName, const string& timeStr)
{
  try
  {
    // Expecting "timestamptz '<datetime>'"

    const char* timestamptz = "timestamptz";
    const char* TIMESTAMPTZ = "TIMESTAMPTZ";
    const char* c = timeStr.c_str();

    while (*c == ' ')
    {
      c++;
    }

    while (*c && *timestamptz && ((*c == *timestamptz) || (*c == *TIMESTAMPTZ)))
    {
      c++;
      timestamptz++;
      TIMESTAMPTZ++;
    }

    if ((!(*timestamptz)) && (*c == ' '))
    {
      do
      {
        c++;
      } while (*c == ' ');

      const char* c2 = nullptr;
      if (*c == '\'')
        c2 = strchr(c + 1, '\'');

      if ((c2 != nullptr) && (c2 > (c + 1)))
      {
        const auto* c3 = c2;

        do
        {
          c3++;
        } while (*c3 == ' ');

        if (!(*c3))
        {
          string ts;

          c++;
          ts.assign(c, c2 - c);

          return Fmi::TimeParser::parse(ts);
        }
      }
    }

    throw Fmi::Exception(
        BCP, timeName + " \"" + timeStr + "\" is not valid, timestamptz '<datetime>' expected");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace

void EngineImpl::validateTimes(const QueryOptions& queryOptions) const
{
  try
  {
    // Time range or observation time is required, having "timestamptz '<datetime>'" value(s)

    auto const& timeOptions = queryOptions.itsTimeOptions;

    if (timeOptions.itsStartTime.empty() != timeOptions.itsEndTime.empty())
      throw Fmi::Exception(BCP, "'starttime' and 'endtime' options must be given simultaneously");

    if (!timeOptions.itsStartTime.empty())
    {
      if (!timeOptions.itsObservationTime.empty())
        throw Fmi::Exception(
            BCP,
            "Can't specify both time range ('starttime' and 'endtime') and observation time "
            "('time')");

      auto st = parseTime("starttime", timeOptions.itsStartTime);
      auto et = parseTime("endtime", timeOptions.itsEndTime);

      if (st > et)
        throw Fmi::Exception(BCP, "'starttime' must be earlier than 'endtime'");
    }
    else if (!timeOptions.itsObservationTime.empty())
    {
      auto s = Fmi::ascii_tolower_copy(boost::algorithm::trim_copy(timeOptions.itsObservationTime));

      if (s != "current_timestamp")
        parseTime("time", timeOptions.itsObservationTime);
    }
    else
      throw Fmi::Exception(BCP, "Query starttime and endtime or observation time must be given");

    if (queryOptions.itsTimeOptions.itsMessageTimeChecks)
      queryOptions.itsTimeOptions.itsMessageCreatedTime.clear();
    else
      queryOptions.itsTimeOptions.itsMessageCreatedTime = "current_timestamp";
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Check requested parameters are known
 */
// ----------------------------------------------------------------------

void EngineImpl::validateParameters(const StringList& paramList,
                                    Validity validity,
                                    bool& messageColumnSelected) const
{
  try
  {
    // All parameters must be known but don't have to be selectable (validity controls whether
    // selecting stations and/or accepted messages or rejected messages). At least one parameter
    // must be selectable. Duplicates are not allowed.
    //
    // 'messageColumnSelected' is set on return if any selectable message column is requested
    // (the information is used when querying stations)

    if (paramList.empty())
      throw Fmi::Exception(BCP, "The 'param'option is missing or empty!");

    Columns columns;
    bool paramKnown, duplicate;

    for (auto const& param : paramList)
    {
      QueryTable* queryTables = messageQueryTables;
      paramKnown = false;

      for (QueryTable* qt = queryTables;;)
      {
        const QueryTable& queryTable = *qt;

        if (queryTable.itsName.empty())
        {
          if (queryTables == messageQueryTables)
          {
            qt = queryTables = rejectedMessageQueryTables;
            continue;
          }
          else if (queryTables == rejectedMessageQueryTables)
          {
            qt = queryTables = firQueryTables;
            continue;
          }

          break;
        }

        auto queryColumn = getQueryColumn(queryTable.itsColumns, columns, param, duplicate);

        if (queryColumn)
        {
          columns.emplace_back(*queryColumn);
          paramKnown = true;

          break;
        }
        else if (duplicate)
          throw Fmi::Exception(BCP, "Duplicate 'param' name '" + param + "'");

        qt++;
      }

      if (!paramKnown)
        throw Fmi::Exception(BCP, "Unknown 'param' name '" + param + "'");
    }

    string selectClause;
    bool distinct = false;

    if ((validity == Validity::Accepted) || (validity == Validity::AcceptedMessages))
    {
      // Must have at least 1 station or accepted message column for 'Accepted' and at least 1
      // message column for 'AcceptedMessages'
      //
      buildMessageQuerySelectClause(messageQueryTables,
                                    StationIdList(),
                                    StringList(),
                                    paramList,
                                    false,
                                    selectClause,
                                    messageColumnSelected,
                                    distinct);

      if ((validity == Validity::AcceptedMessages) && (!messageColumnSelected))
        selectClause.clear();
    }
    else
    {
      // Must have at least 1 rejected message column
      //
      buildMessageQuerySelectClause(rejectedMessageQueryTables,
                                    StationIdList(),
                                    StringList(),
                                    paramList,
                                    false,
                                    selectClause,
                                    messageColumnSelected,
                                    distinct);
    }

    if (selectClause.empty())
      throw Fmi::Exception(BCP, "No applicable 'param' names given");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Check given station id's exist
 */
// ----------------------------------------------------------------------

void EngineImpl::validateStationIds(const Fmi::Database::PostgreSQLConnection& connection,
                                    const StationIdList& stationIdList,
                                    bool debug) const
{
  try
  {
    if (stationIdList.empty())
      return;

    ostringstream selectFromWhereClause;

    selectFromWhereClause << "SELECT request_stations.station_id FROM (VALUES ";

    for (size_t n = 1; (n <= stationIdList.size()); n++)
      selectFromWhereClause << ((n == 1) ? "($" : ",($") << n << ((n == 1) ? "::integer)" : ")");

    selectFromWhereClause << ") AS request_stations (station_id) LEFT JOIN avidb_stations ON "
                             "request_stations.station_id = avidb_stations.station_id "
                          << "WHERE avidb_stations.station_id IS NULL LIMIT 1";

    QueryData queryData;

    queryData.itsColumns.emplace_back(ColumnType::Integer, "station_id");

    executeParamQuery<QueryData, StationIdList>(
        connection, selectFromWhereClause.str(), stationIdList, debug, queryData);

    if (!queryData.itsValues.empty())
    {
      const auto* id = std::get_if<int>(&queryData.itsValues["station_id"].front());
      const int stationId = id ? *id : -1;
      throw Fmi::Exception(BCP, "Unknown station id " + Fmi::to_string(stationId));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Check given icao codes exist
 */
// ----------------------------------------------------------------------

void EngineImpl::validateIcaos(const Fmi::Database::PostgreSQLConnection& connection,
                               const StringList& icaoList,
                               bool debug) const
{
  try
  {
    if (icaoList.empty())
      return;

    ostringstream selectFromWhereClause;

    selectFromWhereClause << "SELECT request_icaos.icao_code FROM (VALUES ";

    for (size_t n = 1; (n <= icaoList.size()); n++)
      selectFromWhereClause << ((n == 1) ? "($" : "),($") << n;

    selectFromWhereClause << ")) AS request_icaos (icao_code) LEFT JOIN avidb_stations ON "
                             "UPPER(request_icaos.icao_code) = UPPER(avidb_stations.icao_code) "
                          << "WHERE avidb_stations.icao_code IS NULL LIMIT 1";

    QueryData queryData;

    queryData.itsColumns.emplace_back(ColumnType::String, "icao_code");

    executeParamQuery<QueryData, StringList>(
        connection, selectFromWhereClause.str(), icaoList, debug, queryData);

    if (!queryData.itsValues.empty())
    {
      const auto* ptr = std::get_if<std::string>(&(queryData.itsValues["icao_code"].front()));
      string icaoCode = ptr ? *ptr : "?";
      throw Fmi::Exception(BCP, "Unknown icao code " + icaoCode).disableLogging();
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Check given icao filters
 */
// ----------------------------------------------------------------------

void EngineImpl::validateIcaoFilters(const LocationOptions& locationOptions) const
{
  try
  {
    auto const& includeIcaoFilters = locationOptions.itsIncludeIcaoFilters;
    auto const& excludeIcaoFilters = locationOptions.itsExcludeIcaoFilters;

    if (includeIcaoFilters.empty() && excludeIcaoFilters.empty())
      return;

    if (locationOptions.itsCountries.empty() && locationOptions.itsWKTs.itsWKTs.empty())
    {
      // edr sets filters for queries which do not use them; ignore them instead of error
      //
      return;

      /*
      throw Fmi::Exception(
          BCP, "Icao code filters are applicable with country code and polygon queries only"
                          ).disableLogging();
      */
    }

    for (auto const& filter : includeIcaoFilters)
      if (filter.empty() || (filter.size() > 4) || strpbrk(filter.c_str(), "%_"))
        throw Fmi::Exception(BCP, "1-4 letter icao code filter expected, no wildcards: " + filter)
            .disableLogging();

    for (auto const& filter : excludeIcaoFilters)
      if (filter.empty() || (filter.size() > 4) || strpbrk(filter.c_str(), "%_"))
        throw Fmi::Exception(BCP, "1-4 letter icao code filter expected, no wildcards: " + filter)
            .disableLogging();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Check given places (station names) exist
 */
// ----------------------------------------------------------------------

void EngineImpl::validatePlaces(const Fmi::Database::PostgreSQLConnection& connection,
                                StringList& placeNameList,
                                bool debug) const
{
  try
  {
    if (placeNameList.empty())
      return;

    ostringstream selectFromWhereClause;

    /* Since non-existing station names has been allowed, just strip them off
     * instead of throwing an error

    selectFromWhereClause << "SELECT request_stations.name FROM (VALUES ";

    for (size_t n = 1; (n <= placeNameList.size()); n++)
      selectFromWhereClause << ((n == 1) ? "($" : "),($") << n;

    selectFromWhereClause
        << ")) AS request_stations (name) LEFT JOIN avidb_stations ON "
           "UPPER(request_stations.name) = UPPER(BTRIM(avidb_stations.name)) "
        << "WHERE avidb_stations.name IS NULL LIMIT 1";

    QueryData queryData;

    queryData.itsColumns.push_back(Column(String, "name"));

    executeParamQuery<QueryData, StringList>(
        connection, selectFromWhereClause.str(), placeNameList, debug, queryData);

    if (!queryData.itsValues.empty())
    {
      string stationName(boost::get<std::string>(&(queryData.itsValues["name"].front()))
                           ? *(boost::get<std::string>(&(queryData.itsValues["name"].front())))
                           : "?");
      throw Fmi::Exception(BCP, "Unknown station name " + stationName).disableLogging();
    }
    */

    StringList places(placeNameList);
    placeNameList.clear();

    for (auto const& place : places)
    {
      QueryData queryData;

      queryData.itsColumns.emplace_back(ColumnType::String, "name");

      selectFromWhereClause << "SELECT request_stations.name FROM (VALUES ($1)) "
                            << "AS request_stations (name) LEFT JOIN avidb_stations ON "
                               "UPPER(request_stations.name) = UPPER(BTRIM(avidb_stations.name)) "
                            << "WHERE avidb_stations.name IS NULL";

      executeParamQuery<QueryData>(
          connection, selectFromWhereClause.str(), place, debug, queryData);

      if (queryData.itsValues.empty())
        placeNameList.push_back(place);

      selectFromWhereClause.str("");
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Check given country codes exist
 */
// ----------------------------------------------------------------------

void EngineImpl::validateCountries(const Fmi::Database::PostgreSQLConnection& connection,
                                   const StringList& countryList,
                                   bool debug) const
{
  try
  {
    if (countryList.empty())
      return;

    ostringstream selectFromWhereClause;

    selectFromWhereClause << "WITH request_countries AS (SELECT country_code FROM (VALUES ";

    for (size_t n = 1; (n <= countryList.size()); n++)
      selectFromWhereClause << ((n == 1) ? "($" : "),($") << n;

    selectFromWhereClause
        << ")) AS request_countries (country_code)) SELECT country_code FROM request_countries "
        << "WHERE NOT EXISTS (SELECT station_id FROM avidb_stations WHERE "
           "request_countries.country_code = UPPER(avidb_stations.country_code)) LIMIT 1";

    QueryData queryData;

    queryData.itsColumns.emplace_back(ColumnType::String, "country_code");

    executeParamQuery<QueryData, StringList>(
        connection, selectFromWhereClause.str(), countryList, debug, queryData);

    if (!queryData.itsValues.empty())
    {
      const auto* ptr = std::get_if<std::string>(&(queryData.itsValues["country_code"].front()));
      string countryCode = ptr ? *ptr : "?";
      throw Fmi::Exception(BCP, "Unknown country code " + countryCode);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Check given wkt's are valid and of supported type.
 *        Convert POINTs to latlons to support 'max # of nearest station' search.
 */
// ----------------------------------------------------------------------

void EngineImpl::validateWKTs(const Fmi::Database::PostgreSQLConnection& connection,
                              LocationOptions& locationOptions,
                              bool debug) const
{
  try
  {
    if (locationOptions.itsWKTs.itsWKTs.empty())
      return;

    // Get type, validity and index (position in itsWKTs collection), and latitude and longitude of
    // POINT definitions for the wkt's.
    // To ease the handling of result rows sort invalid wkts and POINTs to come first.

    ostringstream selectFromWhereClause;

    selectFromWhereClause << "SELECT wkt,geomtype,isvalid,index,"
                          << "CASE geomtype WHEN 'ST_Point' THEN ST_Y(geom) ELSE 0 END AS lat,"
                             "CASE geomtype WHEN 'ST_Point' THEN ST_X(geom) ELSE 0 END AS lon "
                          << "FROM (SELECT wkt,ST_GeomFromText(wkt,4326) AS geom,"
                             "ST_GeometryType(ST_GeomFromText(wkt,4326)) AS geomtype,"
                          << "CASE WHEN NOT ST_IsValid(ST_GeomFromText(wkt,4326)) OR "
                          << "ST_GeometryType(ST_GeomFromText(wkt,4326)) NOT IN "
                             "('ST_Point','ST_Polygon','ST_LineString') "
                          << "THEN 0 ELSE 1 END AS isvalid,index FROM (VALUES ";

    size_t wktCnt = locationOptions.itsWKTs.itsWKTs.size();

    for (size_t n = 1; (n <= wktCnt); n++)
      selectFromWhereClause << ((n == 1) ? "($" : "),($") << n << "," << n - 1;

    selectFromWhereClause
        << ")) AS request_wkts (wkt,index)) AS wkts ORDER BY isvalid,CASE geomtype "
           "WHEN 'ST_Point' THEN 0 ELSE 1 END,index";

    // If a single LINESTRING (route) is given, the stations (and their messages) will be ordered by
    // route segment index and station's distance to the start of the segment. Otherwise icao code
    // order is used

    bool checkIfRoute =
        (locationOptions.itsLonLats.empty() && locationOptions.itsStationIds.empty() &&
         locationOptions.itsIcaos.empty() && locationOptions.itsCountries.empty() &&
         locationOptions.itsPlaces.empty() && locationOptions.itsBBoxes.empty() && (wktCnt == 1));

    QueryData queryData;

    queryData.itsColumns.emplace_back(ColumnType::String, "wkt");
    queryData.itsColumns.emplace_back(ColumnType::String, "geomtype");
    queryData.itsColumns.emplace_back(ColumnType::Integer, "isvalid");
    queryData.itsColumns.emplace_back(ColumnType::Integer, "index");
    queryData.itsColumns.emplace_back(ColumnType::Double, "lat");
    queryData.itsColumns.emplace_back(ColumnType::Double, "lon");

    executeParamQuery<QueryData, StringList>(
        connection, selectFromWhereClause.str(), locationOptions.itsWKTs.itsWKTs, debug, queryData);

    if (queryData.itsValues["wkt"].size() != wktCnt)
      throw Fmi::Exception(
          BCP, "validateWKTs: internal: wkt check query did not return as many rows as expected");

    bool isValid = value_or<int>(queryData.itsValues["isvalid"].front(), 0);

    if (!isValid)
    {
      auto wkt = value_or<std::string>(queryData.itsValues["wkt"].front(), "?");

      const auto geomType = value_or<std::string>(queryData.itsValues["geomtype"].front(), "?");

      if ((geomType == "ST_Point") || (geomType == "ST_Polygon") || (geomType == "ST_LineString"))
        throw Fmi::Exception(BCP, "Invalid wkt " + wkt);

      throw Fmi::Exception(
          BCP, "Unsupported wkt " + wkt + "; use POINT(s), POLYGON(s) or LINESTRING(s)");
    }

    // Convert POINTs to latlons to support 'max # of nearest station' search

    auto itwkt = locationOptions.itsWKTs.itsWKTs.begin();
    string geomType;

    for (int wktIndex = 0, dataIndex = 0; (wktCnt > 0); wktCnt--, wktIndex++, dataIndex++)
    {
      geomType = value_or<std::string>(queryData.itsValues["geomtype"][dataIndex], "?");

      if (geomType != "ST_Point")
        break;

      int index = value_or<int>(queryData.itsValues["index"][dataIndex], -1);

      if (index >= 0)
      {
        for (; ((wktIndex < index) && (itwkt != locationOptions.itsWKTs.itsWKTs.end())); wktIndex++)
          itwkt++;
      }

      if ((index < 0) || (itwkt == locationOptions.itsWKTs.itsWKTs.end()))
        throw Fmi::Exception(BCP, "validateWKTs: internal: wkt index is invalid");

      auto lat = value_or<double>(queryData.itsValues["lat"][dataIndex], 0);
      auto lon = value_or<double>(queryData.itsValues["lon"][dataIndex], 0);

      locationOptions.itsLonLats.emplace_back(lon, lat);

      itwkt = locationOptions.itsWKTs.itsWKTs.erase(itwkt);
    }

    // Set 'route' flag if the one and only wkt is a LINESTRING

    locationOptions.itsWKTs.isRoute = (checkIfRoute && (geomType == "ST_LineString"));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations
 */
// ----------------------------------------------------------------------
//
// private
//
StationQueryData EngineImpl::queryStations(const Fmi::Database::PostgreSQLConnection& connection,
                                           QueryOptions& queryOptions,
                                           bool validateQuery) const
{
  try
  {
    // Validate requested times, parameters, station id's, icao codes, country codes and wkts

    auto const& paramList = queryOptions.itsParameters;
    auto& locationOptions = queryOptions.itsLocationOptions;

    if (validateQuery)
    {
      validateParameters(paramList, Validity::Accepted, queryOptions.itsMessageColumnSelected);

      if (!locationOptions.itsStationIds.empty())
        validateStationIds(connection, locationOptions.itsStationIds, queryOptions.itsDebug);

      if (!locationOptions.itsIcaos.empty())
        validateIcaos(connection, locationOptions.itsIcaos, queryOptions.itsDebug);

      if ((!locationOptions.itsIncludeIcaoFilters.empty()) ||
          (!locationOptions.itsExcludeIcaoFilters.empty()))
        validateIcaoFilters(locationOptions);

      if (!locationOptions.itsPlaces.empty())
        validatePlaces(connection, locationOptions.itsPlaces, queryOptions.itsDebug);

      if (!locationOptions.itsCountries.empty())
        validateCountries(connection, locationOptions.itsCountries, queryOptions.itsDebug);

      if ((!locationOptions.itsWKTs.itsWKTs.empty()) && (!locationOptions.itsWKTs.isRoute))
        validateWKTs(connection, locationOptions, queryOptions.itsDebug);
    }

    // If any message column is requested, select only stationid and additionally distance/bearing
    // if requested (all other requested station columns except distance/bearing are selected by
    // message query) and queryOptions.messageColumnSelected is set instructing the caller to call
    // queryMessages() with the returned station id's
    //
    // Build select column expressions
    //
    // distance is automatically selected if querying with coordinates and max # of nearest stations

    StationQueryData stationQueryData;
    bool selectStationListOnly = queryOptions.itsMessageColumnSelected;
    bool autoSelectDistance =
        ((!locationOptions.itsLonLats.empty()) && (locationOptions.itsNumberOfNearestStations > 0));
    bool firIdQuery;
    string selectClause;

    stationQueryData.itsColumns = buildStationQuerySelectClause(
        paramList, selectStationListOnly, autoSelectDistance, selectClause, firIdQuery);

    // Query separately with each type of location options, adding the unique results to 'queryData'
    //
    // Note: Query with coordinates must be executed first to get distance and bearing values

    if (!locationOptions.itsLonLats.empty())
      queryStationsWithCoordinates(connection,
                                   locationOptions,
                                   queryOptions.itsMessageTypes,
                                   selectClause,
                                   queryOptions.itsDebug,
                                   stationQueryData);

    if (!locationOptions.itsStationIds.empty())
      queryStationsWithIds(connection,
                           locationOptions.itsStationIds,
                           selectClause,
                           queryOptions.itsDebug,
                           stationQueryData);

    if (!locationOptions.itsIcaos.empty())
      queryStationsWithIcaos(connection,
                             locationOptions.itsIcaos,
                             selectClause,
                             firIdQuery,
                             queryOptions.itsDebug,
                             stationQueryData);

    if (!locationOptions.itsCountries.empty())
      queryStationsWithCountries(connection,
                                 locationOptions.itsCountries,
                                 locationOptions.itsExcludeIcaoFilters,
                                 selectClause,
                                 firIdQuery,
                                 queryOptions.itsDebug,
                                 stationQueryData);

    if (!locationOptions.itsPlaces.empty())
      queryStationsWithPlaces(connection,
                              locationOptions.itsPlaces,
                              selectClause,
                              queryOptions.itsDebug,
                              stationQueryData);

    if (!locationOptions.itsWKTs.itsWKTs.empty())
      queryStationsWithWKTs(connection,
                            locationOptions,
                            queryOptions.itsMessageTypes,
                            selectClause,
                            queryOptions.itsDebug,
                            stationQueryData);

    if (!locationOptions.itsBBoxes.empty())
      queryStationsWithBBoxes(
          connection, locationOptions, selectClause, queryOptions.itsDebug, stationQueryData);

    if (!queryOptions.itsMessageColumnSelected)
    {
      // There are no message related columns.
      //
      // Sort the columns and erase automatically selected columns (distance) if such exist
      //
      if (autoSelectDistance)
      {
        sortColumnList(stationQueryData.itsColumns);

        for (auto it = stationQueryData.itsColumns.begin();
             (it != stationQueryData.itsColumns.end());)
        {
          if (it->itsSelection == ColumnSelection::Automatic)
            it = stationQueryData.itsColumns.erase(it);
          else
            it++;
        }
      }
    }

    return stationQueryData;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

//
// public api stub
//
StationQueryData EngineImpl::queryStations(QueryOptions& queryOptions) const
{
  try
  {
    auto connectionPtr = itsConnectionPool->get();
    auto& connection = *connectionPtr.get();

    queryOptions.itsLocationOptions.itsWKTs.isRoute = false;

    return queryStations(connection, queryOptions, true);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Check given message types are known
 */
// ----------------------------------------------------------------------

void EngineImpl::validateMessageTypes(const Fmi::Database::PostgreSQLConnection& /*connection*/,
                                      const StringList& messageTypeList,
                                      bool /*debug*/) const
{
  try
  {
    if (messageTypeList.empty())
      return;

    // Checking against configuration (not the database)

    auto it_begin = itsConfig->getMessageTypes().begin();
    auto it_end = itsConfig->getMessageTypes().end();

    for (auto const& msgType : messageTypeList)
      if (find(it_begin, it_end, msgType) == it_end)
        throw Fmi::Exception(BCP, "Unknown message type " + msgType);

    /*
    ostringstream selectFromWhereClause;

    size_t n = 0;

    for (auto const & messageType : messageTypeList) {
            selectFromWhereClause << ((n == 0) ? "SELECT request_message_types.type FROM (VALUES
    (quote_literal('" : "')),(quote_literal('") << Fmi::ascii_toupper_copy(messageType);
            n++;
    }

    selectFromWhereClause << "'))) AS request_message_types (type) LEFT JOIN avidb_message_types ON
    BTRIM(request_message_types.type,'''') = UPPER(avidb_message_types.type) "
                                              << "WHERE avidb_message_types.type IS NULL LIMIT 1";

    QueryData queryData;

    queryData.itsColumns.push_back(Column(String,"type"));

    executeQuery<QueryData>(connection,selectFromWhereClause.str(),debug,queryData);

    if (!queryData.itsValues.empty()) {
            string msgType(boost::get<std::string>(&(queryData.itsValues["type"].front())) ?
    *(boost::get<std::string>(&(queryData.itsValues["type"].front()))) : "?");
            throw Fmi::Exception(BCP,"Unknown message type " + msgType);
    }
    */
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Get pointer to internal column definition for given
 *        datetime column in avidb_messages table
 */
// ----------------------------------------------------------------------

const Column* EngineImpl::getMessageTableTimeColumn(const string& timeColumn) const
{
  try
  {
    Columns columns;
    bool duplicate = false;

    auto queryColumn = getQueryColumn(messageQueryColumns, columns, timeColumn, duplicate);

    if ((!queryColumn) || (queryColumn->itsType != ColumnType::DateTime))
      throw Fmi::Exception(
          BCP, string("Column '") + timeColumn + "' is not a datetime column in message table");

    return queryColumn;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query accepted messages
 */
// ----------------------------------------------------------------------
//
// private
//
StationQueryData EngineImpl::queryMessages(const Fmi::Database::PostgreSQLConnection& connection,
                                           const StationIdList& stationIdList,
                                           const QueryOptions& queryOptions,
                                           bool validateQuery) const
{
  try
  {
    // Check # of stations and validate requested parameters and message types

    int maxStations =
        (queryOptions.itsMaxMessageStations >= 0 ? queryOptions.itsMaxMessageStations
                                                 : itsConfig->getMaxMessageStations());

    if ((maxStations > 0) && ((int)stationIdList.size() > maxStations))
      throw Fmi::Exception(BCP,
                           string("Max number of stations exceeded (") +
                               Fmi::to_string(maxStations) + "/" +
                               Fmi::to_string(stationIdList.size()) + "), limit the query");

    bool messageColumnSelected = queryOptions.itsMessageColumnSelected;
    const Column* timeRangeColumn = nullptr;

    if (validateQuery)
    {
      validateTimes(queryOptions);

      validateParameters(
          queryOptions.itsParameters, Validity::AcceptedMessages, messageColumnSelected);

      if (!queryOptions.itsMessageTypes.empty())
        validateMessageTypes(connection, queryOptions.itsMessageTypes, queryOptions.itsDebug);
    }

    // If querying messages created within time range, get the column to be used for time
    // restriction (message_time by default, not settable currently)

    if (queryOptions.itsTimeOptions.itsObservationTime.empty() &&
        (!queryOptions.itsTimeOptions.itsQueryValidRangeMessages))
      timeRangeColumn =
          getMessageTableTimeColumn(queryOptions.itsTimeOptions.getMessageTableTimeRangeColumn());

    // Build select column expressions

    bool routeQuery = queryOptions.itsLocationOptions.itsWKTs.isRoute,
         distinct = queryOptions.itsDistinctMessages;
    string selectClause;

    TableMap tableMap = buildMessageQuerySelectClause(messageQueryTables,
                                                      stationIdList,
                                                      queryOptions.itsMessageTypes,
                                                      queryOptions.itsParameters,
                                                      routeQuery,
                                                      selectClause,
                                                      messageColumnSelected,
                                                      distinct);

    // Build column list and sort the columns to the requested order

    StationQueryData stationQueryData;

    for (auto const& table : tableMap)
    {
      for (auto const& column : table.second.itsSelectedColumns)
        stationQueryData.itsColumns.push_back(column);
    }

    sortColumnList(stationQueryData.itsColumns);

    string withClause;

    if ((!stationIdList.empty()) && queryOptions.itsLocationOptions.itsWKTs.isRoute)
    {
      // Build 'request_stations' table ('WITH table AS ...') for input station id's',
      // containing 'position' column for sorting the stations to route order
      //
      withClause = buildRequestStationsWithClause(stationIdList,
                                                  queryOptions.itsLocationOptions.itsWKTs.isRoute);

      // Add 'request_stations' into tablemap for joining into main query

      auto& table = tableMap[requestStationsTable.itsName];
      table.itsAlias = requestStationsTable.itsAlias;
      table.itsJoin = requestStationsTable.itsJoin;
    }

    // If querying valid messages (in contrast to querying messages created within the given time
    // range) the query is generated based on 'record_set' CTE using message_time restriction and
    // thus enabling use of an index. Otherwise an indexed time column
    // (queryOptions.itsTimeOptions.itsTimeRangeColumn) is expected to be used for 'created within'
    // query; the query is generated directly against avidb_messages table

    bool filterMETARs = queryOptions.itsFilterMETARs && itsConfig->getFilterFIMETARxxx();
    bool excludeSPECIs = queryOptions.itsExcludeSPECIs;

    if (!queryOptions.itsTimeOptions.itsObservationTime.empty() ||
        queryOptions.itsTimeOptions.itsQueryValidRangeMessages)
    {
      // Build WITH clause for 'record_set' table containg all valid messages for given time
      // instant/range.
      //
      // Note: By passing false instead of queryOptions.itsLocationOptions.itsWKTs.isRoute
      // record_set.station_id is limited by IN (StationIdList) instead by subquery from
      // request_stations table which makes the query slower (request_stations CTE is generated
      // only for route query)

      string recordSetWithClause;

      if (queryOptions.itsTimeOptions.itsObservationTime.empty())
        recordSetWithClause =
            buildRecordSetWithClause(!queryOptions.itsLocationOptions.itsBBoxes.empty(),
                                     false /*queryOptions.itsLocationOptions.itsWKTs.isRoute*/,
                                     stationIdList,
                                     queryOptions.itsMessageFormat,
                                     queryOptions.itsMessageTypes,
                                     itsConfig->getMessageTypes(),
                                     itsConfig->getRecordSetStartTimeOffsetHours(),
                                     itsConfig->getRecordSetEndTimeOffsetHours(),
                                     queryOptions.itsTimeOptions.itsStartTime,
                                     queryOptions.itsTimeOptions.itsEndTime);
      else
        recordSetWithClause =
            buildRecordSetWithClause(!queryOptions.itsLocationOptions.itsBBoxes.empty(),
                                     false /*queryOptions.itsLocationOptions.itsWKTs.isRoute*/,
                                     stationIdList,
                                     queryOptions.itsMessageFormat,
                                     queryOptions.itsMessageTypes,
                                     itsConfig->getMessageTypes(),
                                     itsConfig->getRecordSetStartTimeOffsetHours(),
                                     itsConfig->getRecordSetEndTimeOffsetHours(),
                                     queryOptions.itsTimeOptions.itsObservationTime);

      withClause += ((withClause.empty() ? "WITH " : ",") + recordSetWithClause);

      // Build WITH clause for 'message_validity' table containing configured message type specific
      // validity length for message types having MessageValidTimeRange[Latest] or
      // MessageTimeRange[Latest] restriction

      string messageValidityWithClause = buildMessageTypeValidityWithClause(
          queryOptions.itsMessageTypes, itsConfig->getMessageTypes());

      if (!messageValidityWithClause.empty())
      {
        withClause += ("," + messageValidityWithClause);

        if (queryOptions.itsTimeOptions.itsObservationTime.empty())
        {
          // If needed, add 'message_validity' into tablemap for joining into main query
          // ('latest_messages' and 'messagetimerangelatest_messages' queries join
          // to it by itself when needed).
          //
          list<TimeRangeType> timeRangeTypes;
          timeRangeTypes.push_back(TimeRangeType::ValidTimeRange);
          timeRangeTypes.push_back(TimeRangeType::ValidTimeRangeLatest);
          timeRangeTypes.push_back(TimeRangeType::CreationValidTimeRange);
          timeRangeTypes.push_back(TimeRangeType::CreationValidTimeRangeLatest);
          timeRangeTypes.push_back(TimeRangeType::MessageValidTimeRange);
          timeRangeTypes.push_back(TimeRangeType::MessageValidTimeRangeLatest);
          timeRangeTypes.push_back(TimeRangeType::MessageTimeRange);

          string noMessageValidityTypesIn = buildMessageTypeInClause(
              queryOptions.itsMessageTypes, itsConfig->getMessageTypes(), timeRangeTypes);

          if (!noMessageValidityTypesIn.empty())
          {
            //
            // Because 'message_validity' contains data only for message types queried with
            // MessageValidTimeRange[Latest] or MessageTimeRange[Latest] time restriction,
            // it needs to be outer joined with avidb_message_types in the main query for querying
            // message types with other time restriction types
            //
            auto& table = tableMap[messageValidityTable.itsName];
            table.itsAlias = messageValidityTable.itsAlias;
            table.itsJoin = messageValidityTable.itsJoin;

            timeRangeTypes.pop_back();  // Erase MessageTimeRange, MessageValidTimeRangeLatest and
                                        // MessageValidTimeRange
            timeRangeTypes.pop_back();  //
            timeRangeTypes.pop_back();  //

            noMessageValidityTypesIn = buildMessageTypeInClause(
                queryOptions.itsMessageTypes, itsConfig->getMessageTypes(), timeRangeTypes);

            table.leftOuter = (!noMessageValidityTypesIn.empty());
          }
          else
          {
            // Only MessageTimeRangeLatest types queried, no need to add 'message_validity' into the
            // tablemap
          }
        }
      }

      // If querying with observation time, build WITH clause for 'latest_messages' table containing
      // message id's for the latest messages.
      //
      // Otherwise if querying valid MessageTimeRangeLatest messages, build WITH clause for
      // 'messagetimerangelatest_messages' table containing message id's for the
      // latest messages having message_time earlier than range start time in addition to all
      // messages having starttime <= message_time < endtime

      if (!queryOptions.itsTimeOptions.itsObservationTime.empty())
      {
        // Build WITH clause for 'latest_messages' table
        //
        withClause +=
            ("," + buildLatestMessagesWithClause(queryOptions.itsMessageTypes,
                                                 itsConfig->getMessageTypes(),
                                                 queryOptions.itsTimeOptions.itsObservationTime,
                                                 queryOptions.itsTimeOptions.itsMessageCreatedTime,
                                                 filterMETARs,
                                                 excludeSPECIs,
                                                 queryOptions.itsDistinctMessages,
                                                 itsConfig->getFilterFIMETARxxxExcludeIcaos()));

        // Add 'latest_messages' into tablemap
        //
        // Note: 'latest_messages' is used thru subquery because of poor performance if joined.

        auto& table = tableMap[latestMessagesTable.itsName];
        table.itsJoin = latestMessagesTable.itsJoin;
        table.subQuery = true;
      }
      else
        withClause +=
            buildMessageTimeRangeMessagesWithClause(queryOptions.itsMessageTypes,
                                                    itsConfig->getMessageTypes(),
                                                    queryOptions.itsTimeOptions.itsStartTime,
                                                    queryOptions.itsTimeOptions.itsEndTime,
                                                    filterMETARs,
                                                    excludeSPECIs,
                                                    itsConfig->getFilterFIMETARxxxExcludeIcaos());

      withClause += " ";

      // Replace avidb_messages with record_set in the tablemap; record_set contains all (columns
      // of) needed avidb_messages rows, and is used instead

      auto it = tableMap.find(messageTableName);

      if (it == tableMap.end())
        throw Fmi::Exception(BCP, "queryMessages: internal: Unable to get avidb_messages table");

      auto recordSetTable = it->second;
      tableMap.erase(it);

      tableMap[recordSetTableName] = recordSetTable;
    }
    else
    {
      if (queryOptions.itsLocationOptions.itsWKTs.isRoute)
      {
        withClause += " ";

        if (filterMETARs || excludeSPECIs)
        {
          // Ensure station table is joined into main query for METAR filtering or exclusion of
          // SPECIs (for nonroute query it's joined anyways for ordering the rows by icao code)
          //
          string messageTypeIn = buildMessageTypeInClause(
              queryOptions.itsMessageTypes, itsConfig->getMessageTypes(), list<TimeRangeType>());

          if ((filterMETARs && (messageTypeIn.find("'METAR") != string::npos)) ||
              (excludeSPECIs && (messageTypeIn.find("'SPECI'") != string::npos)))
          {
            auto& table = tableMap[stationTableName];

            if (table.itsAlias.empty())
              table.itsAlias = stationTableAlias;

            if (table.itsJoin.empty())
              table.itsJoin = stationTableJoin;
          }
        }
      }

      // Filter by message format

      auto& table = tableMap[messageFormatTableName];
      table.itsAlias = messageFormatTableAlias;
      table.itsJoin = queryOptions.itsMessageFormat == "TAC" ? messageFormatTableJoinTAC
                                                             : messageFormatTableJoinIWXXM;
    }

    // For time range query ensure tablemap contains message_types table for joining into main
    // query; needed for message type or type specific time range restriction

    if (queryOptions.itsTimeOptions.itsObservationTime.empty())
    {
      auto& table = tableMap[messageTypeTableName];

      if (table.itsAlias.empty())
        table.itsAlias = messageTypeTableAlias;

      if (table.itsJoin.empty())
        table.itsJoin = messageTypeTableJoin;
    }

    // Max row count for the query; if exceeded, an error is thrown; if <= 0, unlimited

    int maxMessageRows = (queryOptions.itsMaxMessageRows >= 0 ? queryOptions.itsMaxMessageRows
                                                              : itsConfig->getMaxMessageRows());

    // Build from, where and order by clause (by avidb_stations.icao_code or by route segment index
    // and station's distance to the start of the segment) and execute query

    ostringstream fromWhereOrderByClause;

    buildMessageQueryFromWhereOrderByClause(maxMessageRows,
                                            stationIdList,
                                            queryOptions,
                                            tableMap,
                                            *itsConfig,
                                            timeRangeColumn,
                                            distinct,
                                            fromWhereOrderByClause);

    executeQuery<StationQueryData>(connection,
                                   withClause + selectClause + fromWhereOrderByClause.str(),
                                   queryOptions.itsDebug,
                                   stationQueryData,
                                   queryOptions.itsDistinctMessages,
                                   maxMessageRows);

    return stationQueryData;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}
//
// public api stub
//
StationQueryData EngineImpl::queryMessages(const StationIdList& stationIdList,
                                           const QueryOptions& queryOptions) const
{
  try
  {
    auto connectionPtr = itsConnectionPool->get();
    auto& connection = *connectionPtr.get();

    return queryMessages(connection, stationIdList, queryOptions, true);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Join station data to another by station id
 */
// ----------------------------------------------------------------------

StationQueryData& EngineImpl::joinStationAndMessageData(const StationQueryData& stationData,
                                                        StationQueryData& messageData) const
{
  try
  {
    // If distance or bearing is available, copy the values from station data to message data

    bool hasDistance = ((find(stationData.itsColumns.begin(),
                              stationData.itsColumns.end(),
                              stationDistanceQueryColumn) != stationData.itsColumns.end()) &&
                        (find(messageData.itsColumns.begin(),
                              messageData.itsColumns.end(),
                              stationDistanceQueryColumn) != messageData.itsColumns.end()));
    bool hasBearing = ((find(stationData.itsColumns.begin(),
                             stationData.itsColumns.end(),
                             stationBearingQueryColumn) != stationData.itsColumns.end()) &&
                       (find(messageData.itsColumns.begin(),
                             messageData.itsColumns.end(),
                             stationBearingQueryColumn) != messageData.itsColumns.end()));

    if (hasDistance || hasBearing)
    {
      for (auto const& station : stationData.itsValues)
        // Ignore stations having no messages.
        //
        // Note: The input data's have station id as a map key. If the 'station' data contains
        // distance and/or bearing, copy the values to 'message' data
        //
        if (messageData.itsValues.find(station.first) != messageData.itsValues.end())
        {
          if (hasDistance)
          {
            // Copy nonnull distance value to all message rows for the station
            //
            auto const& distanceValue =
                station.second.find(stationDistanceQueryColumn)->second.front();

            const double* distancePtr = std::get_if<double>(&distanceValue);
            if (distancePtr)
            {
              auto stationDistance = *distancePtr;

              for (auto& distance :
                   messageData.itsValues[station.first][stationDistanceQueryColumn])
                distance = stationDistance;
            }
          }

          if (hasBearing)
          {
            // Copy bearing value to all message rows for the station
            //
            auto const& bearingValue =
                station.second.find(stationBearingQueryColumn)->second.front();

            if (const double* bearingPtr = std::get_if<double>(&bearingValue))
            {
              auto stationBearing = *bearingPtr;

              for (auto& bearing : messageData.itsValues[station.first][stationBearingQueryColumn])
                bearing = stationBearing;
            }
          }
        }
    }

    return messageData;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations and/or accepted messages.
 *
 *        This method serves for convenience, by executing station and message queries
 *        and joining the data.
 */
// ----------------------------------------------------------------------

StationQueryData EngineImpl::queryStationsAndMessages(QueryOptions& queryOptions) const
{
  try
  {
    if (queryOptions.itsValidity == Validity::Rejected)
      throw Fmi::Exception(
          BCP,
          "queryStationsAndMessages() can't be used to query rejected messages; use "
          "queryRejectedMessages() instead");

    validateTimes(queryOptions);

    // Query station scoped, FIR scped and global scoped stations and messages.
    //
    // If route query (single linestring wkt) is requested, query each scope separately;
    // otherwise fetch all messages (message types) with single query

    auto connectionPtr = itsConnectionPool->get();
    auto& connection = *connectionPtr.get();

    StringList queryMessageTypes(queryOptions.itsMessageTypes.begin(),
                                 queryOptions.itsMessageTypes.end());

    StationQueryData stationScopeStations, firScopeStations, globalScopeStations;
    StationQueryData stationScopeMessages, firScopeMessages, globalScopeMessages;
    StationQueryData* data = nullptr;

    struct ScopeData
    {
      MessageScope scope;
      StationQueryData& stationData;
      StationQueryData& messageData;
    };

    MessageScope stationOrAll = MessageScope::NoScope;

    if (!queryOptions.itsLocationOptions.itsWKTs.itsWKTs.empty())
    {
      // Check for route query; whether to use scoped queries or single query for all message types

      validateWKTs(connection, queryOptions.itsLocationOptions, queryOptions.itsDebug);

      if (queryOptions.itsLocationOptions.itsWKTs.isRoute)
        stationOrAll = MessageScope::StationScope;
    }

    // Validate all requested message types (without taking scope into account)

    validateMessageTypes(connection, queryMessageTypes, queryOptions.itsDebug);

    list<ScopeData> scopeDatas = {
        {stationOrAll, stationScopeStations, stationScopeMessages},
        {MessageScope::FIRScope, firScopeStations, firScopeMessages},
        {MessageScope::GlobalScope, globalScopeStations, globalScopeMessages}};

    bool validateQuery = true;

    for (auto& scope : scopeDatas)
    {
      scopeMessageTypes(queryMessageTypes,
                        itsConfig->getMessageTypes(),
                        scope.scope,
                        queryOptions.itsMessageTypes);

      if (!queryOptions.itsMessageTypes.empty())
      {
        // Query stations

        scope.stationData = queryStations(connection, queryOptions, validateQuery);
        validateQuery = false;

        if ((scope.scope == MessageScope::GlobalScope) || !scope.stationData.itsStationIds.empty())
        {
          // Query messages if any message column were requested

          if (queryOptions.itsMessageColumnSelected)
          {
            // Query messages and join station and message data to get distance and bearing values
            // for message data rows
            //
            scope.messageData = queryMessages(
                connection, scope.stationData.itsStationIds, queryOptions, validateQuery);
            joinStationAndMessageData(scope.stationData, scope.messageData);

            // Collect/combine data

            if (!data)
              data = &scope.messageData;

            if (data != &scope.messageData)
              for (const auto& station : scope.messageData.itsValues)
              {
                auto its = data->itsValues.insert(make_pair(station.first, QueryValues()));

                if (its.second)
                  data->itsStationIds.push_back(station.first);

                for (auto column : station.second)
                {
                  auto itc = its.first->second.insert(make_pair(column.first, ValueVector()));
                  itc.first->second.insert(
                      itc.first->second.end(), column.second.begin(), column.second.end());
                }
              }
          }
          else
          {
            if (data)
              data->itsValues.insert(scope.stationData.itsValues.begin(),
                                     scope.stationData.itsValues.end());

            data = &scope.stationData;
          }
        }
      }

      if (scope.scope == MessageScope::NoScope)
        break;
    }

    return data ? *data : stationScopeStations;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query rejected messages
 */
// ----------------------------------------------------------------------

QueryData EngineImpl::queryRejectedMessages(const QueryOptions& queryOptions) const
{
  try
  {
    // Validate time options, parameters and message types

    if (!queryOptions.itsTimeOptions.itsObservationTime.empty())
      throw Fmi::Exception(BCP, "Time range must be used to query rejected messages");

    validateTimes(queryOptions);

    auto connectionPtr = itsConnectionPool->get();
    auto& connection = *connectionPtr.get();

    bool messageColumnSelected;

    validateParameters(queryOptions.itsParameters, Validity::Rejected, messageColumnSelected);

    if (!queryOptions.itsMessageTypes.empty())
      validateMessageTypes(connection, queryOptions.itsMessageTypes, queryOptions.itsDebug);

    // Build select column expressions

    string selectClause;
    bool distinct = false;

    TableMap tableMap = buildMessageQuerySelectClause(rejectedMessageQueryTables,
                                                      StationIdList(),
                                                      queryOptions.itsMessageTypes,
                                                      queryOptions.itsParameters,
                                                      false,
                                                      selectClause,
                                                      messageColumnSelected,
                                                      distinct);

    // Build column list and sort the columns to the requested order

    QueryData queryData;

    for (auto const& table : tableMap)
    {
      for (auto const& column : table.second.itsSelectedColumns)
        queryData.itsColumns.push_back(column);
    }

    sortColumnList(queryData.itsColumns);

    // Max row count for the query; if exceeded, an error is thrown; if <= 0, unlimited

    int maxMessageRows = (queryOptions.itsMaxMessageRows >= 0 ? queryOptions.itsMaxMessageRows
                                                              : itsConfig->getMaxMessageRows());

    // Build from, where and order by clause (by rejected_messages.icao_code) and execute query

    ostringstream fromWhereOrderByClause;

    buildRejectedMessageQueryFromWhereOrderByClause(
        maxMessageRows, queryOptions, tableMap, fromWhereOrderByClause);

    executeQuery<QueryData>(connection,
                            selectClause + fromWhereOrderByClause.str(),
                            queryOptions.itsDebug,
                            queryData,
                            false,
                            maxMessageRows);

    return queryData;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query FIR areas
 */
// ----------------------------------------------------------------------

const FIRQueryData& EngineImpl::queryFIRAreas() const
{
  try
  {
    auto ptr = itsFIRAreasPtr.load(std::memory_order_relaxed);

    if (ptr)
      return *ptr;

    std::unique_lock<std::mutex> lock(itsFIRMutex);

    loadFIRAreas();

    if (!itsFIRAreas.empty())
      itsFIRAreasPtr.store(&itsFIRAreas, std::memory_order_relaxed);

    return itsFIRAreas;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Load/store FIR areas
 */
// ----------------------------------------------------------------------

void EngineImpl::loadFIRAreas() const
{
  try
  {
    auto connectionPtr = itsConnectionPool->get();
    auto& connection = *connectionPtr.get();

    string query(
        "SELECT gid, ST_AsGeoJSON(ST_ForcePolygonCCW(areageom)) AS geom,"
        "ST_XMin(areageom) AS xmin,ST_YMin(areageom) AS ymin,"
        "ST_XMax(areageom) AS xmax,ST_YMax(areageom) AS ymax"
        " FROM icao_fir_yhdiste ORDER BY 1");

    auto result = connection.executeNonTransaction(query);

    for (pqxx::result::const_iterator row = result.begin(); (row != result.end()); row++)
    {
      auto gid = row["gid"].as<int>();
      auto geom = row["geom"].as<string>();
      auto xmin = row["xmin"].as<double>();
      auto ymin = row["ymin"].as<double>();
      auto xmax = row["xmax"].as<double>();
      auto ymax = row["ymax"].as<double>();

      BBox bbox(xmin, xmax, ymin, ymax);

      itsFIRAreas.insert(std::make_pair(gid, std::make_pair(geom, bbox)));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet

// DYNAMIC MODULE CREATION TOOLS

extern "C" void* engine_class_creator(const char* theConfigFileName, void* /* user_data */)
{
  // return new SmartMet::Engine::Avi::EngineImpl(theConfigFileName);
  {
    try
    {
      using SmartMet::Spine::log_time_str;
      const bool disabled = [&theConfigFileName]()
      {
        const char* name = "SmartMet::Engine::Avi::Engine::create";
        if (theConfigFileName == nullptr || *theConfigFileName == 0)
        {
          std::cout << log_time_str() << ' ' << ANSI_FG_RED << name
                    << ": configuration file not specified or its name is empty string: "
                    << "engine disabled." << ANSI_FG_DEFAULT << '\n';
          return true;
        }

        SmartMet::Spine::ConfigBase cfg(theConfigFileName);
        const bool result = cfg.get_optional_config_param<bool>("disabled", false);
        if (result)
          std::cout << log_time_str() << ' ' << ANSI_FG_RED << name << ": engine disabled"
                    << ANSI_FG_DEFAULT << '\n';
        return result;
      }();

      if (disabled)
        return new SmartMet::Engine::Avi::Engine();

      return new SmartMet::Engine::Avi::EngineImpl(theConfigFileName);
    }
    catch (...)
    {
      throw Fmi::Exception::Trace(BCP, "Operation failed!");
    }
  }
}

extern "C" const char* engine_name()
{
  return "Avi";
}
// ======================================================================
