#pragma once

#include "ColumnList.h"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
namespace
{
boost::local_time::time_zone_ptr tzUTC(new boost::local_time::posix_time_zone("UTC"));

// ----------------------------------------------------------------------
/*!
 * \brief Return derived column select expression
 */
// ----------------------------------------------------------------------

std::string derivedExpression(const std::string& tableColumnName,
                              const std::string& queryColumnName)
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

std::string nullExpression(const Column* queryColumn)
{
  // NULL AS distance
  // NULL AS bearing

  return std::string("NULL AS ") + queryColumn->itsName;
}

// Derived column expressions

const char* const dfLongitude = "ST_X(geom)";
const char* const dfLatitude = "ST_Y(geom)";
const char* const dfLonLat = "('' || ST_X(geom) || ',' || ST_Y(geom))";
const char* const dfLatLon = "('' || ST_Y(geom) || ',' || ST_X(geom))";
const char* const dfDistance =
    "ST_Distance(geom::geography,ST_SetSRID(coordinates.coordinate,4326)::geography) / 1000";
const char* const dfBearing = "DEGREES(ST_Azimuth(geom,ST_SetSRID(coordinates.coordinate,4326)))";

// Some table/column etc. names/joins

const char* const stationTableName = "avidb_stations";
const char* const stationTableAlias = "st";
const char* const stationTableJoin = "st.station_id = me.station_id";
const char* const stationIcaoTableColumn = "icao_code";
const char* const stationIcaoQueryColumn = "icao";
const char* const stationDistanceQueryColumn = "distance";
const char* const stationBearingQueryColumn = "bearing";
const char* const stationCoordinateColumn = "coordinate";
const char* const stationLonLatQueryColumn = "lonlat";
const char* const stationLatLonQueryColumn = "latlon";
const char* const messageIdTableColumn = "message_id";
const char* const messageTableColumn = messageQueryColumn;
const char* const rejectedMessageIcaoTableColumn = "icao_code";
const char* const recordSetTableName = "record_set";
const char* const messageTableName = "avidb_messages";
const char* const messageTableAlias = "me";
const char* const messageTypeTableName = "avidb_message_types";
const char* const messageTypeTableAlias = "mt";
const char* const messageTypeTableJoin = "me.type_id = mt.type_id";
const char* const messageRouteTableName = "avidb_message_routes";
const char* const messageRouteTableAlias = "mr";
const char* const messageRouteTableJoin = "me.route_id = mr.route_id";
const char* const rejectedMessageTableName = "avidb_rejected_messages";
const char* const rejectedMessageTableAlias = messageTableAlias;
const char* const rejectedMessageTypeTableJoin = messageTypeTableJoin;
const char* const rejectedMessageRouteTableJoin = messageRouteTableJoin;
const char* const latestMessagesTableJoin =
    "me.message_id IN (SELECT message_id FROM latest_messages)";
const char* const requestStationsTableAlias = "rs";
const char* const requestStationsPositionColumn = "position";
const char* const requestStationsTableJoin = "rs.station_id = me.station_id";
const char* const messageValidityTableName = "message_validity";
const char* const messageValidityTableAlias = "mv";
const char* const messageValidityTableJoin = "mv.type = mt.type";
const char* const messageTimeRangeLatestMessagesTableName = "messagetimerangelatest_messages";

// Table/query column mapping

Column stationQueryColumns[] = {
    //
    // Table columns
    //
    // Type		 Table column	   		 Query column
    //
    {Integer, "station_id", stationIdQueryColumn},
    {String, stationIcaoTableColumn, stationIcaoQueryColumn},
    {String, "name", "name"},
    {Integer, "elevation", "elevation"},
    {DateTime, "valid_from", "stationvalidfrom"},
    {DateTime, "valid_to", "stationvalidto"},
    {DateTime, "modified_last", "stationmodified"},
    {String, "country_code", "iso2"},
    //
    // Derived columns
    //
    // Type		 Derived column    Query column				   f(Table column)
    // f(Table column)
    //
    {Double, dfLongitude, "longitude", derivedExpression, nullptr},
    {Double, dfLatitude, "latitude", derivedExpression, nullptr},
    {TS_LonLat, dfLonLat, stationLonLatQueryColumn, derivedExpression, nullptr},
    {TS_LatLon, dfLatLon, stationLatLonQueryColumn, derivedExpression, nullptr},
    {Double, dfDistance, stationDistanceQueryColumn, nullptr, derivedExpression},
    {Double, dfBearing, stationBearingQueryColumn, nullptr, derivedExpression},
    {None, "", "", nullptr, nullptr}};

Column messageTypeQueryColumns[] = {
    //
    // Table columns
    //
    // Type		 Table column	   Query column
    //
    {String, "type", "messagetype"},
    {String, "description", "messagetypedescription"},
    {DateTime, "modified_last", "messagetypemodified"},
    {None, "", ""}};

Column messageRouteQueryColumns[] = {
    //
    // Table columns
    //
    // Type		 Table column	   Query column
    //
    {String, "name", "route"},
    {String, "description", "routedescription"},
    {DateTime, "modified_last", "routemodified"},
    {None, "", ""}};

Column messageQueryColumns[] = {
    //
    // Table columns
    //
    // Type		 Table column			  Query column
    //
    {Integer, "station_id", stationIdQueryColumn},
    {Integer, messageIdTableColumn, "messageid"},
    {String, messageTableColumn, messageQueryColumn},
    {DateTime, "message_time", "messagetime"},
    {DateTime, "valid_from", "messagevalidfrom"},
    {DateTime, "valid_to", "messagevalidto"},
    {DateTime, "created", "messagecreated"},
    {DateTime, "file_modified", "messagefilemodified"},
    {String, "messir_heading", "messirheading"},
    {String, "version", "messageversion"},
    {None, "", ""}};

Column rejectedMessageQueryColumns[] = {
    //
    // Table columns
    //
    // Type		 Table column			  Query column
    //
    {String, "icao_code", "messagerejectedicao"},
    {String, "message", "message"},
    {DateTime, "message_time", "messagetime"},
    {DateTime, "valid_from", "messagevalidfrom"},
    {DateTime, "valid_to", "messagevalidto"},
    {DateTime, "created", "messagecreated"},
    {DateTime, "file_modified", "messagefilemodified"},
    {String, "messir_heading", "messirheading"},
    {String, "version", "messageversion"},
    {Integer, "reject_reason", "messagerejectedreason"},
    {None, "", ""}};

QueryTable messageQueryTables[] = {
    {messageTableName, messageTableAlias, messageQueryColumns, ""},
    {messageTypeTableName, messageTypeTableAlias, messageTypeQueryColumns, messageTypeTableJoin},
    {messageRouteTableName,
     messageRouteTableAlias,
     messageRouteQueryColumns,
     messageRouteTableJoin},
    {stationTableName, stationTableAlias, stationQueryColumns, stationTableJoin},
    {"", "", nullptr, ""}};

Column noQueryColumns[] = {};

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
}  // namespace
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
