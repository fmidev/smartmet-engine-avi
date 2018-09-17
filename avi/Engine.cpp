// ======================================================================

#include "Engine.h"
#include "Connection.h"
#include "ConstantValues.h"
#include <boost/algorithm/string/trim.hpp>
#include <macgyver/StringConversion.h>
#include <spine/Exception.h>
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
// ----------------------------------------------------------------------
/*!
 * \brief Build from and where clause with given coordinates and max distance
 *		  for querying stations
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
        (find(messageTypes.begin(), messageTypes.end(), messageTypeSpecialToNearestStationSearch) ==
         messageTypes.end()))
      fromWhereClause << " AND (" << stationIcaosSpecialToNearestStationSearch << ")";
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build where clause with given icao codes or places (station names) for querying stations
 */
// ----------------------------------------------------------------------

void buildStationQueryWhereClause(const string& columnExpression,
                                  bool quoteLiteral,
                                  const StringList& stringList,
                                  ostringstream& whereClause)
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build from and where (and order by for route query) clause with given wkts for querying
 * stations
 */
// ----------------------------------------------------------------------

void buildStationQueryFromWhereOrderByClause(const LocationOptions& locationOptions,
                                             ostringstream& fromWhereOrderByClause)
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
      ostringstream condition;

      if (locationOptions.itsWKTs.isRoute)
        // For a route (a single linestring), limit the stations by their shortest distance to route
        // segments
        //
        condition << "ST_DWithin(geom::geography,ST_ClosestPoint(segment,geom)::geography," << fixed
                  << setprecision(0) << locationOptions.itsMaxDistance << ")";
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

    // For a route (a single linestring), order the stations by route segment index and station's
    // distance to the start of the segment

    if (locationOptions.itsWKTs.isRoute)
      fromWhereOrderByClause
          << " ORDER BY segindex,ST_Distance(segstart::geography,geom::geography)";
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build where clause with given bboxes for querying stations
 */
// ----------------------------------------------------------------------

void buildStationQueryWhereClause(const BBoxList& bboxList,
                                  double maxDistance,
                                  ostringstream& whereClause)
{
  try
  {
    if (bboxList.empty())
      return;

    whereClause << (whereClause.str().empty() ? "WHERE " : " OR ");

    size_t n = 0;

    for (auto const& bbox : bboxList)
    {
      ostringstream condition;

      condition << "(ST_Length(ST_ShortestLine(geom,ST_SetSRID(ST_MakeBox2D(ST_Point("
                << setprecision(10) << bbox.itsWest << "," << bbox.itsSouth << "),ST_Point("
                << bbox.itsEast << "," << bbox.itsNorth << ")),4326))::geography) <= " << fixed
                << setprecision(0) << maxDistance << ")";

      whereClause << ((n == 0) ? "" : " OR ") << condition.str();

      n++;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build 'request_stations' table (WITH clause) for request station id's
 * 		  (and position/order) for message query
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build 'message_validity' table (WITH clause) for requested message
 * 		  types and their configured validity in hours
 */
// ----------------------------------------------------------------------

string buildMessageTypeValidityWithClause(const StringList messageTypeList,
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

          for (list<string>::const_iterator it = knownTypes.begin(); (it != knownTypes.end());
               it++, n++)
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
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

          for (list<string>::const_iterator it = knownTypes.begin(); (it != knownTypes.end());
               it++, n++)
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build 'record_set' table (WITH clause) for querying accepted messages
 * 		 for given observation time or time range
 */
// ----------------------------------------------------------------------

string buildRecordSetWithClause(bool routeQuery,
                                const StationIdList& stationIdList,
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

    if (!routeQuery)
      buildMessageQueryWhereStationIdInClause(stationIdList, withClause);
    else
      withClause << " WHERE " << messageTableAlias << ".station_id IN (SELECT station_id FROM "
                 << requestStationsTable.itsName << ")";

    whereStationIdIn = withClause.str();
    withClause.str("");
    withClause.clear();

    withClause << recordSetTableName << " AS (SELECT * FROM " << messageTableName << " "
               << messageTableAlias << whereStationIdIn;

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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build GROUP BY expression for message type with given message types
 *		  (e.g. METREP,SPECIAL) and time range type for querying latest accepted messages
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

            for (list<string>::const_iterator it = knownTypes.begin(); (it != knownTypes.end());
                 it++, n++, nn++)
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
                list<string>::const_iterator it = knownTypes.begin();

                for (; (it != knownTypes.end()); it++)
                  if (find(messageTypeList.begin(), messageTypeList.end(), *it) ==
                      messageTypeList.end())
                    break;

                if (it == knownTypes.end())
                {
                  size_t nn = 0;

                  for (list<string>::const_iterator it = knownTypes.begin();
                       (it != knownTypes.end());
                       it++, n++, nn++)
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build GROUP BY expression for messir_heading with given message types
 *		  (e.g. GAFOR) and time range type for querying latest accepted messages
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

          if (messirPatterns.size() >= 1)
          {
            // We don't expect/support 'grouped' messagetypes (e.g. METREP,SPECIAL) currently
            //
            size_t nn = 1;

            groupBy << whenMessageTypeIs << knownTypes.front() << "' THEN CASE";

            for (list<string>::const_iterator it = messirPatterns.begin();
                 (it != messirPatterns.end());
                 it++, nn++)
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

              if (messirPatterns.size() >= 1)
              {
                size_t nn = 1;

                groupBy << whenMessageTypeIs << messageType << "' THEN CASE";

                for (list<string>::const_iterator it = messirPatterns.begin();
                     (it != messirPatterns.end());
                     it++, nn++)
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build 'latest_messages' table (WITH clause) for querying latest
 *		  accepted messages for given observation time
 */
// ----------------------------------------------------------------------

string buildLatestMessagesWithClause(const StringList& messageTypes,
                                     const MessageTypes& knownMessageTypes,
                                     const string& observationTime,
                                     bool filterFIMETARxxx,
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

    withClause << latestMessagesTable.itsName << " AS (";

    string messageTypeIn =
        buildMessageTypeInClause(messageTypes, knownMessageTypes, ValidTimeRangeLatest);

    if (!messageTypeIn.empty())
    {
      // Depending on configuration the latest message for group(s) of types might be returned
      //
      string messageTypeGroupByExpr =
          buildMessageTypeGroupByExpr(messageTypes, knownMessageTypes, ValidTimeRangeLatest);

      // Depending on configuration messir_heading LIKE pattern(s) might be used for additional
      // grouping

      string messirHeadingGroupByExpr =
          buildMessirHeadingGroupByExpr(messageTypes, knownMessageTypes, ValidTimeRangeLatest);

      withClause << "SELECT MAX(message_id) AS message_id FROM record_set " << messageTableAlias
                 << ",avidb_message_types mt"
                 << " WHERE " << messageTypeTableJoin << " AND " << messageTypeIn << " AND "
                 << observationTime << " BETWEEN " << messageTableAlias << ".valid_from AND "
                 << messageTableAlias << ".valid_to"
                 << " AND " << observationTime << " >= " << messageTableAlias << ".created"
                 << " GROUP BY " << messageTableAlias << ".station_id," << messageTypeGroupByExpr
                 << messirHeadingGroupByExpr;

      unionOrEmpty = " UNION ALL ";
    }

    messageTypeIn =
        buildMessageTypeInClause(messageTypes, knownMessageTypes, MessageValidTimeRangeLatest);

    if (!messageTypeIn.empty())
    {
      string messirHeadingGroupByExpr =
          buildMessirHeadingGroupByExpr(messageTypes, knownMessageTypes, MessageTimeRangeLatest);

      withClause << unionOrEmpty << "SELECT MAX(message_id) AS message_id FROM record_set "
                 << messageTableAlias << ",avidb_message_types mt"
                 << "," << messageValidityTable.itsName << " " << messageValidityTableAlias
                 << " WHERE " << messageTypeTableJoin << " AND " << messageTypeIn << " AND (("
                 << observationTime << " BETWEEN " << messageTableAlias << ".message_time AND "
                 << messageTableAlias << ".valid_to) OR (" << messageTableAlias
                 << ".valid_from IS NULL AND " << messageTableAlias << ".valid_to IS NULL AND "
                 << observationTime << " BETWEEN " << messageTableAlias << ".message_time AND "
                 << messageTableAlias << ".message_time + " << messageValidityTableAlias
                 << ".validityhours)) AND " << observationTime << " >= " << messageTableAlias
                 << ".created"
                 << " GROUP BY " << messageTableAlias << ".station_id,mt.type_id"
                 << messirHeadingGroupByExpr;

      unionOrEmpty = " UNION ALL ";
    }

    messageTypeIn =
        buildMessageTypeInClause(messageTypes, knownMessageTypes, MessageTimeRangeLatest);

    if (!messageTypeIn.empty())
    {
      string messageTypeGroupByExpr =
          buildMessageTypeGroupByExpr(messageTypes, knownMessageTypes, MessageTimeRangeLatest);
      string messirHeadingGroupByExpr =
          buildMessirHeadingGroupByExpr(messageTypes, knownMessageTypes, MessageTimeRangeLatest);
      string whereOrAnd = " WHERE ";

      withClause << unionOrEmpty << "SELECT MAX(message_id) AS message_id FROM record_set "
                 << messageTableAlias << ",avidb_message_types mt"
                 << "," << messageValidityTable.itsName << " " << messageValidityTableAlias;

      if (filterFIMETARxxx && (messageTypeIn.find("'METAR") != string::npos))
      {
        withClause << "," << stationTableName << " " << stationTableAlias << " WHERE "
                   << stationTableJoin << " AND ((st.country_code != 'FI' OR mt.type != 'METAR' OR "
                   << messageTableAlias << ".message LIKE 'METAR%')";

        if (!filterFIMETARxxxExcludeIcaos.empty())
          withClause << " OR st.icao_code IN (" << getStringList(filterFIMETARxxxExcludeIcaos)
                     << ")";

        withClause << ")";

        whereOrAnd = " AND ";
      }

      withClause << whereOrAnd << messageTypeTableJoin << " AND " << messageTypeIn << " AND "
                 << messageValidityTableJoin << " AND " << observationTime << " BETWEEN "
                 << messageTableAlias << ".message_time AND " << messageTableAlias
                 << ".message_time + " << messageValidityTableAlias << ".validityhours"
                 << " AND " << observationTime << " >= " << messageTableAlias << ".created"
                 << " GROUP BY " << messageTableAlias << ".station_id," << messageTypeGroupByExpr
                 << messirHeadingGroupByExpr;

      unionOrEmpty = " UNION ALL ";
    }

    messageTypeIn =
        buildMessageTypeInClause(messageTypes, knownMessageTypes, CreationValidTimeRangeLatest);

    if (!messageTypeIn.empty())
    {
      string messirHeadingGroupByExpr = buildMessirHeadingGroupByExpr(
          messageTypes, knownMessageTypes, CreationValidTimeRangeLatest);

      withClause << unionOrEmpty << "SELECT MAX(message_id) AS message_id FROM record_set "
                 << messageTableAlias << ",avidb_message_types mt"
                 << " WHERE " << messageTypeTableJoin << " AND " << messageTypeIn << " AND "
                 << observationTime << " BETWEEN " << messageTableAlias << ".created AND "
                 << messageTableAlias << ".valid_to"
                 << " GROUP BY " << messageTableAlias << ".station_id" << messirHeadingGroupByExpr;

      unionOrEmpty = " UNION ALL ";
    }

    messageTypeIn = buildMessageTypeInClause(messageTypes, knownMessageTypes, ValidTimeRange);

    if (!messageTypeIn.empty())
    {
      withClause << unionOrEmpty << "SELECT message_id FROM record_set " << messageTableAlias
                 << ",avidb_message_types mt"
                 << " WHERE " << messageTypeTableJoin << " AND " << messageTypeIn << " AND "
                 << observationTime << " BETWEEN " << messageTableAlias << ".valid_from AND "
                 << messageTableAlias << ".valid_to"
                 << " AND " << observationTime << " >= " << messageTableAlias << ".created";

      unionOrEmpty = " UNION ALL ";
    }

    messageTypeIn = buildMessageTypeInClause(messageTypes, knownMessageTypes, MessageTimeRange);

    if (!messageTypeIn.empty())
    {
      withClause << unionOrEmpty << "SELECT message_id FROM record_set " << messageTableAlias
                 << ",avidb_message_types mt"
                 << "," << messageValidityTable.itsName << " " << messageValidityTableAlias
                 << " WHERE " << messageTypeTableJoin << " AND " << messageTypeIn << " AND "
                 << messageValidityTableJoin << " AND " << observationTime << " BETWEEN "
                 << messageTableAlias << ".message_time AND " << messageTableAlias
                 << ".message_time + " << messageValidityTableAlias << ".validityhours"
                 << " AND " << observationTime << " >= " << messageTableAlias << ".created";

      unionOrEmpty = " UNION ALL ";
    }

    messageTypeIn =
        buildMessageTypeInClause(messageTypes, knownMessageTypes, CreationValidTimeRange);

    if (!messageTypeIn.empty())
      withClause << unionOrEmpty << "SELECT message_id FROM record_set " << messageTableAlias
                 << ",avidb_message_types mt"
                 << " WHERE " << messageTypeTableJoin << " AND " << messageTypeIn << " AND "
                 << observationTime << " BETWEEN " << messageTableAlias << ".created AND "
                 << messageTableAlias << ".valid_to";

    withClause << ")";

    return withClause.str();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build 'messagetimerangelatest_messages' table (WITH clause) for
 *		  querying accepted messages having MessageTimeRangeLatest time
 *		  restriction with given time range
 */
// ----------------------------------------------------------------------

string buildMessageTimeRangeMessagesWithClause(const StringList& messageTypes,
                                               const MessageTypes& knownMessageTypes,
                                               const string& startTime,
                                               const string& endTime,
                                               bool filterFIMETARxxx,
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
    MessageTimeRangeLatest time restriction)
              latest message would be returned for each group's type
    */

    string messageTypeIn =
        buildMessageTypeInClause(messageTypes, knownMessageTypes, MessageTimeRangeLatest);

    if (messageTypeIn.empty())
      return "";

    ostringstream withClause;
    string whereOrAnd = " WHERE ";

    withClause << "," << messageTimeRangeLatestMessagesTableName << " AS ("
               << "SELECT me.message_id "
               << "FROM record_set me,avidb_message_types mt";

    if (filterFIMETARxxx && (messageTypeIn.find("'METAR") != string::npos))
    {
      withClause << "," << stationTableName << " " << stationTableAlias << " WHERE "
                 << stationTableJoin << " AND ((st.country_code != 'FI' OR mt.type != 'METAR' OR "
                 << messageTableAlias << ".message LIKE 'METAR%')";

      if (!filterFIMETARxxxExcludeIcaos.empty())
        withClause << " OR st.icao_code IN (" << getStringList(filterFIMETARxxxExcludeIcaos) << ")";

      withClause << ")";

      whereOrAnd = " AND ";
    }
    else
      filterFIMETARxxx = false;

    withClause << whereOrAnd << "me.type_id = mt.type_id AND " << messageTypeIn
               << " AND me.message_time >= " << startTime << " AND me.message_time < " << endTime
               << " UNION "
               << "SELECT MAX(message_id) AS message_id "
               << "FROM record_set me,avidb_message_types mt,message_validity mv";

    if (filterFIMETARxxx)
    {
      withClause << "," << stationTableName << " " << stationTableAlias << " WHERE "
                 << stationTableJoin << " AND ((st.country_code != 'FI' OR mt.type != 'METAR' OR "
                 << messageTableAlias << ".message LIKE 'METAR%')";

      if (!filterFIMETARxxxExcludeIcaos.empty())
        withClause << " OR st.icao_code IN (" << getStringList(filterFIMETARxxxExcludeIcaos) << ")";

      withClause << ")";
    }

    withClause << whereOrAnd << "me.type_id = mt.type_id AND " << messageTypeIn
               << " AND mv.type = mt.type"
               << " AND me.message_time < " << startTime
               << " AND me.message_time + mv.validityhours > " << startTime
               << " GROUP BY me.station_id,mt.type)";

    return withClause.str();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build from, where and order by clause with given station id's, message types,
 *		  tables and time instant/range for querying accepted messages
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
        // valid_from-valid_to range and any range calculated
        //		 with range start time and period length (here me.message_time +
        // mv.validityhours) are taken as a closed range.
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
        timeRangeTypes.push_back(ValidTimeRange);
        timeRangeTypes.push_back(ValidTimeRangeLatest);

        string messageTypeIn = buildMessageTypeInClause(
            queryOptions.itsMessageTypes, knownMessageTypes, timeRangeTypes);
        string emptyOrOr = "";

        if (!messageTypeIn.empty())
        {
          fromWhereOrderByClause << "(" << messageTypeIn << " AND "
                                 << "(" << queryOptions.itsTimeOptions.itsStartTime
                                 << " <= " << messageTableAlias << ".valid_to"
                                 << " AND " << queryOptions.itsTimeOptions.itsEndTime << " > "
                                 << messageTableAlias << ".valid_from))";
          emptyOrOr = " OR ";
        }

        timeRangeTypes.clear();
        timeRangeTypes.push_back(MessageValidTimeRange);
        timeRangeTypes.push_back(MessageValidTimeRangeLatest);

        messageTypeIn = buildMessageTypeInClause(
            queryOptions.itsMessageTypes, knownMessageTypes, timeRangeTypes);

        if (!messageTypeIn.empty())
        {
          fromWhereOrderByClause << emptyOrOr << "(" << messageTypeIn << " AND "
                                 << "((" << queryOptions.itsTimeOptions.itsStartTime
                                 << " <= " << messageTableAlias << ".valid_to"
                                 << " AND " << queryOptions.itsTimeOptions.itsEndTime << " > "
                                 << messageTableAlias << ".message_time) OR (" << messageTableAlias
                                 << ".valid_from IS NULL AND " << messageTableAlias
                                 << ".valid_to IS NULL"
                                 << " AND "
                                 << "(" << queryOptions.itsTimeOptions.itsStartTime << " <= ("
                                 << messageTableAlias << ".message_time + "
                                 << messageValidityTableAlias << ".validityhours)"
                                 << " AND " << queryOptions.itsTimeOptions.itsEndTime << " > "
                                 << messageTableAlias << ".message_time))))";
          emptyOrOr = " OR ";
        }

        messageTypeIn = buildMessageTypeInClause(
            queryOptions.itsMessageTypes, knownMessageTypes, MessageTimeRange);

        if (!messageTypeIn.empty())
        {
          fromWhereOrderByClause << emptyOrOr << "(" << messageTypeIn << " AND "
                                 << "(" << queryOptions.itsTimeOptions.itsStartTime << " <= ("
                                 << messageTableAlias << ".message_time + "
                                 << messageValidityTableAlias << ".validityhours)"
                                 << " AND " << queryOptions.itsTimeOptions.itsEndTime << " > "
                                 << messageTableAlias << ".message_time))";
          emptyOrOr = " OR ";
        }

        // For MessageTimeRangeLatest restriction querying 'messagetimerangelatest_messages' for the
        // id's of the latest valid messages
        // having message_time earlier than starttime in addition to all messages having starttime
        // <=
        // message_time < endtime
        //
        // Note: Even through 'messagetimerangelatest_messages' contains message id's only for
        // MessageTimeRangeLatest types, type restriction
        //		 must be combined with IN clause below; otherwise query throughput
        // drastically drops with longer time ranges
        //		 (query planner is somehow fooled by the query)

        messageTypeIn = buildMessageTypeInClause(
            queryOptions.itsMessageTypes, knownMessageTypes, MessageTimeRangeLatest);

        if (!messageTypeIn.empty())
        {
          fromWhereOrderByClause << emptyOrOr << "(" << messageTypeIn << " AND ("
                                 << messageTableAlias << ".message_id IN (SELECT message_id FROM "
                                 << messageTimeRangeLatestMessagesTableName << ")))";
          emptyOrOr = " OR ";
        }

        timeRangeTypes.clear();
        timeRangeTypes.push_back(CreationValidTimeRange);
        timeRangeTypes.push_back(CreationValidTimeRangeLatest);

        messageTypeIn = buildMessageTypeInClause(
            queryOptions.itsMessageTypes, knownMessageTypes, timeRangeTypes);

        if (!messageTypeIn.empty())
          fromWhereOrderByClause << emptyOrOr << "(" << messageTypeIn << " AND "
                                 << "(" << queryOptions.itsTimeOptions.itsStartTime
                                 << " <= " << messageTableAlias << ".valid_to"
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
          throw SmartMet::Spine::Exception(
              BCP, "buildMessageQueryFromWhereOrderByClause(): internal: time column is NULL");

        buildMessageQueryWhereStationIdInClause(stationIdList, fromWhereOrderByClause);
        string messageTypeIn = buildMessageTypeInClause(
            queryOptions.itsMessageTypes, knownMessageTypes, list<TimeRangeType>());

        fromWhereOrderByClause << " AND " << messageTypeIn << " AND " << messageTableAlias << "."
                               << timeRangeColumn->getTableColumnName()
                               << " >= " << queryOptions.itsTimeOptions.itsStartTime << " AND "
                               << messageTableAlias << "." << timeRangeColumn->getTableColumnName()
                               << " < " << queryOptions.itsTimeOptions.itsEndTime;

        if (queryOptions.itsFilterMETARs && config.getFilterFIMETARxxx() &&
            (messageTypeIn.find("'METAR") != string::npos))
        {
          // AND (
          //      (st.country_code != 'FI' OR mt.type != 'METAR' OR me.message LIKE 'METAR%')
          //      [ OR st.icao_code IN (ExcludedIcaoList) ]
          //     )

          fromWhereOrderByClause << " AND ((" << stationTableAlias << ".country_code != 'FI' OR "
                                 << messageTypeTableAlias << ".type != 'METAR' OR "
                                 << messageTableAlias << ".message LIKE 'METAR%')";

          auto const& filterFIMETARxxxExcludeIcaos = config.getFilterFIMETARxxxExcludeIcaos();

          if (!filterFIMETARxxxExcludeIcaos.empty())
            fromWhereOrderByClause << " OR st.icao_code IN ("
                                   << getStringList(filterFIMETARxxxExcludeIcaos) << ")";

          fromWhereOrderByClause << ")";
        }
      }
    }
    else
    {
      // Message restriction made by join to latest_messages.message_id
    }

    // ORDER BY { st.icao_code | rs.position } [,me.message] [,me.message_id]

    if (!queryOptions.itsLocationOptions.itsWKTs.isRoute)
      fromWhereOrderByClause << " ORDER BY " << stationTableAlias << "." << stationIcaoTableColumn;
    else
      fromWhereOrderByClause << " ORDER BY " << requestStationsTableAlias << "."
                             << requestStationsPositionColumn;

    if (!distinct)
    {
      if (queryOptions.itsDistinctMessages)
        // Using message for ordering too (needed to check/skip duplicates)
        //
        fromWhereOrderByClause << "," << messageTableAlias << "." << messageTableColumn;

      // Using message id for ordering too (needed to ensure regression tests can succeed)

      fromWhereOrderByClause << "," << messageTableAlias << "." << messageIdTableColumn;
    }

    // [ LIMIT maxMessageRows ]

    if (maxMessageRows > 0)
      fromWhereOrderByClause << " LIMIT " << maxMessageRows + 1;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build from, where and order by clause with given message types,
 *		  message tables and time range for querying rejected messages
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}
}  // anonymous namespace

// ----------------------------------------------------------------------
/*!
 * \brief The only permitted constructor requires a configfile
 */
// ----------------------------------------------------------------------

Engine::Engine(const std::string& theConfigFileName) : itsConfigFileName(theConfigFileName) {}

// ----------------------------------------------------------------------
/*!
 * \brief Initialize the engine
 *
 * Note: We do not wish to delay the initialization of other engines.
 * init() is done in its own thread, hence we read the configuration
 * files here, and hence itsConfig is a pointer instead of an object.
 */
// ----------------------------------------------------------------------

void Engine::init()
{
  try
  {
    itsConfig.reset(new Config(itsConfigFileName));
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Init failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Shutdown the engine
 */
// ----------------------------------------------------------------------

void Engine::shutdown()
{
  std::cout << "  -- Shutdown requested (aviengine)\n";
}

// ----------------------------------------------------------------------
/*!
 * \brief Get column mapping using query column name
 */
// ----------------------------------------------------------------------

const Column* Engine::getQueryColumn(const ColumnTable tableColumns,
                                     ColumnList& columns,
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

    for (const Column* column = tableColumns; (!(column->getName().empty())); column++)
    {
      if (column->getName() == theQueryColumnName)
      {
        for (auto& column : columns)
          if (column.getName() == theQueryColumnName)
          {
            if (column.getSelection() == Automatic)
            {
              column.setSelection(AutomaticRequested);
              column.setNumber(columnNumber);
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build select expressions for coordinate dependant derived fields
 */
// ----------------------------------------------------------------------

string Engine::buildStationQueryCoordinateExpressions(const ColumnList& columns) const
{
  try
  {
    // ST_Distance(geom::geography,ST_SetSRID(coordinates.coordinate,4326)::geography) AS distance
    // DEGREES(ST_Azimuth(geom,ST_SetSRID(coordinates.coordinate,4326))) AS bearing

    ostringstream selectExpressions;

    for (auto const& column : columns)
      if (column.hasCoordinateExpression())
        selectExpressions << "," << column.getCoordinateExpression();

    return selectExpressions.str();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build select clause for querying stations
 */
// ----------------------------------------------------------------------

ColumnList Engine::buildStationQuerySelectClause(const StringList& paramList,
                                                 bool selectStationListOnly,
                                                 bool autoSelectDistance,
                                                 string& selectClause) const
{
  try
  {
    if (paramList.empty())
      throw SmartMet::Spine::Exception(BCP, "No parameters were given to station query");

    // Selected columns

    ColumnList columns;
    bool duplicate;

    selectClause.clear();

    // Station id is automatically selected

    auto queryColumn =
        getQueryColumn(stationQueryColumns, columns, stationIdQueryColumn, duplicate);

    if (!queryColumn)
      throw SmartMet::Spine::Exception(
          BCP, "buildStationQuerySelectClause(): internal: Unable to get id column");

    Column column(*queryColumn);
    column.setSelection(Automatic);
    columns.push_back(column);

    selectClause =
        string("SELECT ") + queryColumn->getTableColumnName() + " AS " + queryColumn->getName();

    // distance is automatically selected to apply max # of nearest stations

    if (autoSelectDistance)
    {
      auto queryColumn =
          getQueryColumn(stationQueryColumns, columns, stationDistanceQueryColumn, duplicate);

      if (!queryColumn)
        throw SmartMet::Spine::Exception(
            BCP, "buildStationQuerySelectClause(): internal: Unable to get distance column");

      Column column(*queryColumn);
      column.setSelection(Automatic);
      columns.push_back(column);
    }

    int columnNumber = 0;

    for (auto const& param : paramList)
    {
      // Note: If 'selectStationList' is set, scanning only for derived distance and bearing columns
      // (they are not available in current message query),
      //		 and setting 'AutomaticRequested' (instead of 'Automatic') type for the user
      // requested
      // fields (stationid) to indicate they shall be returned
      //
      auto queryColumn =
          getQueryColumn(stationQueryColumns, columns, param, duplicate, columnNumber);

      if (queryColumn && ((!selectStationListOnly) || queryColumn->hasCoordinateExpression()))
      {
        if (queryColumn->hasExpression())
        {
          // ST_X(geom) AS longitudes
          // ST_Y(geom) AS latitude
          // ST_X(geom) || ',' || ST_Y(geom) AS lonlat
          // ST_Y(geom) || ',' || ST_X(geom) AS latlon
          //
          selectClause +=
              (string(selectClause.empty() ? "SELECT " : ",") + queryColumn->getExpression());
        }
        else if (queryColumn->hasCoordinateExpression())
        {
          // Parametrized expression for each given coordinate
          //
          // Note: Column expression is generated and added to the select clause only when querying
          // with coordinates (queryStationsWithCoordinates())
          ;
        }
        else
        {
          selectClause +=
              (string(selectClause.empty() ? "SELECT " : ",") + queryColumn->getTableColumnName());

          if (queryColumn->getType() == DateTime)
            selectClause += string(" AT TIME ZONE 'UTC'");

          if ((queryColumn->getType() == DateTime) ||
              (queryColumn->getName() != queryColumn->getTableColumnName()))
            selectClause += (string(" AS ") + queryColumn->getName());
        }

        columns.push_back(*queryColumn);
        columns.back().setNumber(columnNumber);
      }

      columnNumber++;
    }

    return columns;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Build select clause for querying accepted or rejected messages
 */
// ----------------------------------------------------------------------

TableMap Engine::buildMessageQuerySelectClause(QueryTable* queryTables,
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
    // is
    // data map key) and
    // for joining with station data (to get distance and bearing), and for non route query
    // icao_code
    // column is added to
    // the list of query columns to join with station table for ordering the messages by icao code.
    //
    // Message column is automatically selected to check duplicates if any other message table
    // column
    // is selected

    if (queryTables->itsName == messageTableName)
    {
      // Because queries like "param=name,messagetype,route" - selecting columns from
      // avidb_stations,
      // avidb_message_types and avidb_message_routes, but none
      // from avidb_messages - are allowed, the select for accepted messages is distinct unless at
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
            throw SmartMet::Spine::Exception(
                BCP, "buildMessageQuerySelectClause(): internal: Unable to get station id column");

          Column column(*queryColumn);
          column.setSelection(Automatic);
          table.itsSelectedColumns.push_back(column);

          selectClause = queryTable.itsAlias + "." + queryColumn->getTableColumnName() + " AS " +
                         queryColumn->getName();
        }
        else if ((!routeQuery) && (queryTable.itsName == stationTableName))
        {
          auto& table = tableMap[stationTableName];
          table.itsAlias = queryTable.itsAlias;
          table.itsJoin = queryTable.itsJoin;

          auto queryColumn = getQueryColumn(
              queryTable.itsColumns, table.itsSelectedColumns, stationIcaoQueryColumn, duplicate);

          if (!queryColumn)
            throw SmartMet::Spine::Exception(
                BCP,
                "buildMessageQuerySelectClause(): internal: Unable to get station icao column");

          Column column(*queryColumn);
          column.setSelection(Automatic);
          table.itsSelectedColumns.push_back(column);

          // NOTE: Generate select expression if icao is requested by the caller too. When looping
          // the
          // parameters below, column's select
          // 		 expression is NOT generated if the column already exists in column list;
          // it's
          // type just gets changed to AutomaticRequested

          if (find(paramList.begin(), paramList.end(), stationIcaoQueryColumn) != paramList.end())
          {
            selectClause += (string(selectClause.empty() ? "" : ",") + queryTable.itsAlias + "." +
                             queryColumn->getTableColumnName() + " AS " + queryColumn->getName());
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

          if (queryColumn->hasExpression())
          {
            selectClause +=
                (string(selectClause.empty() ? "" : ",") + queryColumn->getExpression());
          }
          else if (queryColumn->hasCoordinateExpression())
          {
            // Select NULL for distance and bearing columns; they will be set after the query by
            // searching from station data
            //
            selectClause += (string(selectClause.empty() ? "" : ",") + nullExpression(queryColumn));
          }
          else
          {
            selectClause += (string(selectClause.empty() ? "" : ",") + queryTable.itsAlias + "." +
                             queryColumn->getTableColumnName());

            if (queryColumn->getType() == DateTime)
              selectClause += string(" AT TIME ZONE 'UTC'");

            if ((queryColumn->getType() == DateTime) ||
                (queryColumn->getName() != queryColumn->getTableColumnName()))
              selectClause += (string(" AS ") + queryColumn->getName());
          }

          if (queryTable.itsName != stationTableName)
          {
            if (queryTable.itsName == messageTableName)
            {
              distinct = false;

              if (distinctMessages && (queryColumn->getName() == messageQueryColumn))
                // Clear flag forcing automatic 'message' column selection
                //
                distinctMessages = false;
              else
                checkDuplicateMessages = true;
            }

            messageColumnSelected = true;
          }

          table.itsSelectedColumns.push_back(*queryColumn);
          table.itsSelectedColumns.back().setNumber(columnNumber);

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
        throw SmartMet::Spine::Exception(
            BCP, "buildMessageQuerySelectClause(): internal: Unable to get message column");

      Column column(*queryColumn);
      column.setSelection(Automatic);
      table.itsSelectedColumns.push_back(column);

      selectClause += (string(",") + messageTableAlias + "." + queryColumn->getTableColumnName() +
                       " AS " + queryColumn->getName());
    }

    // SELECT [DISTINCT] ...
    //
    // Note: "for SELECT DISTINCT, ORDER BY expressions must appear in select list"; ensure
    // avidb_stations is joined and icao is selected for non route query,
    //		 or select 'position' column for route query

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
    else if (routeQuery && distinct)
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Execute query and load the result into given data object
 */
// ----------------------------------------------------------------------

template <typename T>
void Engine::executeQuery(const Connection& connection,
                          const string& query,
                          bool debug,
                          T& queryData,
                          bool distinctRows,
                          int maxRows) const
{
  try
  {
    if (debug)
      cerr << "Query: " << query << std::endl;

    auto result = connection.executeNonTransaction(query);

    if (debug)
      cerr << "Rows: " << result.size() << std::endl;

    if ((maxRows > 0) && ((int)result.size() > maxRows))
      throw SmartMet::Spine::Exception(
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
        // stored as a map key;
        // it is not stored as a column if it was not requested.
        //
        // Also distance is selected automatically for stations to apply max # of nearest stations,
        // and icao is
        // automatically added to the column list to generate station table join to order the
        // messages
        // by icao code
        //
        if (column.getSelection() == Automatic)
          // Column was not requested by the caller, skip it
          //
          continue;

        if (column.getType() == Integer)
        {
          // Currently can't handle NULLs properly, but by setting kFloatMissing for NULL,
          // TableFeeder (used by avi plugin) produces 'missing' (by default, 'nan') column value.
          //
          // Solution is poor, numeric column value 32700 cannot be returned by avi plugin
          // (in practice through, only stationid or messageid could have value 32700).
          //
          SmartMet::Spine::TimeSeries::Value return_value = SmartMet::Spine::TimeSeries::None();
          if (!row[column.getName()].is_null())
            return_value = row[column.getName()].as<int>();

          queryValues[column.getName()].push_back(return_value);
        }
        else if (column.getType() == Double)
        {
          // Note: try/catch; With station query distance and bearing are available only when
          // querying
          // stations with coordinates;
          //		 for other station queries distance and bearing are not selected at all
          //
          SmartMet::Spine::TimeSeries::Value return_value = SmartMet::Spine::TimeSeries::None();
          try
          {
            if (!row[column.getName()].is_null())
              return_value = row[column.getName()].as<double>();
          }
          catch (...)
          {
            return_value = SmartMet::Spine::TimeSeries::None();
          }

          queryValues[column.getName()].push_back(return_value);
        }
        else if (column.getType() == String)
        {
          string strValue;

          bool isNull;

          try
          {
            isNull = row[column.getName()].is_null();
          }
          catch (...)
          {
            isNull = true;
          }

          if (isNull)
            queryValues[column.getName()].push_back(SmartMet::Spine::TimeSeries::None());
          else
            queryValues[column.getName()].push_back(
                boost::algorithm::trim_copy(row[column.getName()].as<string>()));
        }
        else if ((column.getType() == TS_LonLat) || (column.getType() == TS_LatLon))
        {
          // 'latlon' and 'lonlat' are selected as comma separated strings. Return them as
          // SmartMet::Spine::TimeSeries::LonLat for formatted
          // output with TableFeeder
          //
          SmartMet::Spine::TimeSeries::LonLat lonlat(0, 0);
          string llStr(boost::algorithm::trim_copy(row[column.getName()].as<string>()));
          vector<string> flds;
          boost::split(flds, llStr, boost::is_any_of(","));
          bool lonlatValid = false;

          if (flds.size() == 2)
          {
            string& lon = ((column.getName() == stationLonLatQueryColumn) ? flds[0] : flds[1]);
            string& lat = ((column.getName() == stationLonLatQueryColumn) ? flds[1] : flds[0]);

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
            throw SmartMet::Spine::Exception(
                BCP,
                string("Query returned invalid ") + column.getName() + " value '" + llStr + "'");

          queryValues[column.getName()].push_back(lonlat);
        }
        else
        {
          boost::local_time::local_date_time utcTime(
              row[column.getName()].is_null()
                  ? boost::posix_time::ptime()
                  : boost::posix_time::time_from_string(row[column.getName()].as<string>()),
              tzUTC);
          queryValues[column.getName()].push_back(utcTime);
        }
      }
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations with given coordinates
 */
// ----------------------------------------------------------------------

void Engine::queryStationsWithCoordinates(const Connection& connection,
                                          const LocationOptions& locationOptions,
                                          const StringList& messageTypes,
                                          const string& selectClause,
                                          bool debug,
                                          StationQueryData& stationQueryData) const
{
  try
  {
    // Build select expressions for coordinate dependant derived columns

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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations with given station id's
 */
// ----------------------------------------------------------------------

void Engine::queryStationsWithIds(const Connection& connection,
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations with given icao codes
 */
// ----------------------------------------------------------------------

void Engine::queryStationsWithIcaos(const Connection& connection,
                                    const StringList& icaoList,
                                    const string& selectClause,
                                    bool debug,
                                    StationQueryData& stationQueryData) const
{
  try
  {
    // Build where clause and execute query

    ostringstream whereClause;

    buildStationQueryWhereClause("UPPER(icao_code)", false, icaoList, whereClause);

    executeQuery<StationQueryData>(connection,
                                   selectClause + " FROM avidb_stations " + whereClause.str(),
                                   debug,
                                   stationQueryData);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations with given country codes
 */
// ----------------------------------------------------------------------

void Engine::queryStationsWithCountries(const Connection& connection,
                                        const StringList& countryList,
                                        const string& selectClause,
                                        bool debug,
                                        StationQueryData& stationQueryData) const
{
  try
  {
    // Build where clause and execute query

    ostringstream whereClause;

    buildStationQueryWhereClause("UPPER(country_code)", false, countryList, whereClause);

    executeQuery<StationQueryData>(connection,
                                   selectClause + " FROM avidb_stations " + whereClause.str(),
                                   debug,
                                   stationQueryData);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations with given places (station names)
 */
// ----------------------------------------------------------------------

void Engine::queryStationsWithPlaces(const Connection& connection,
                                     const StringList& placeIdList,
                                     const string& selectClause,
                                     bool debug,
                                     StationQueryData& stationQueryData) const
{
  try
  {
    // Build where clause and execute query

    ostringstream whereClause;

    buildStationQueryWhereClause("UPPER(BTRIM(name))", true, placeIdList, whereClause);

    executeQuery<StationQueryData>(connection,
                                   selectClause + " FROM avidb_stations " + whereClause.str(),
                                   debug,
                                   stationQueryData);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations with given wkts
 */
// ----------------------------------------------------------------------

void Engine::queryStationsWithWKTs(const Connection& connection,
                                   const LocationOptions& locationOptions,
                                   const string& selectClause,
                                   bool debug,
                                   StationQueryData& stationQueryData) const
{
  try
  {
    // Build from and where (and order by for route query) clauses and execute query

    ostringstream fromWhereOrderByClause;

    buildStationQueryFromWhereOrderByClause(locationOptions, fromWhereOrderByClause);

    executeQuery<StationQueryData>(
        connection, selectClause + fromWhereOrderByClause.str(), debug, stationQueryData);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations with given bboxes
 */
// ----------------------------------------------------------------------

void Engine::queryStationsWithBBoxes(const Connection& connection,
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Check requested parameters are known
 */
// ----------------------------------------------------------------------

void Engine::validateParameters(const StringList& paramList,
                                Validity validity,
                                bool& messageColumnSelected) const
{
  try
  {
    // All parameters must be known but don't have to be selectable (validity controls whether
    // selecting stations
    // and/or accepted messages or rejected messages). At least one parameter must be selectable.
    // Duplicates are not allowed.
    //
    // 'messageColumnSelected' is set on return if any selectable message column is requested
    // (the information is used when querying stations)

    if (paramList.empty())
      throw SmartMet::Spine::Exception(BCP, "The 'param'option is missing or empty!");

    ColumnList columns;
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

          break;
        }

        auto queryColumn = getQueryColumn(queryTable.itsColumns, columns, param, duplicate);

        if (queryColumn)
        {
          columns.push_back(Column(*queryColumn));
          paramKnown = true;

          break;
        }
        else if (duplicate)
          throw SmartMet::Spine::Exception(BCP, "Duplicate 'param' name '" + param + "'");

        qt++;
      }

      if (!paramKnown)
        throw SmartMet::Spine::Exception(BCP, "Unknown 'param' name '" + param + "'");
    }

    string selectClause;
    bool distinct = false;

    if ((validity == Accepted) || (validity == AcceptedMessages))
    {
      // Must have at least 1 station or accepted message column for 'Accepted' and at least 1
      // message
      // column for 'AcceptedMessages'
      //
      buildMessageQuerySelectClause(messageQueryTables,
                                    StringList(),
                                    paramList,
                                    false,
                                    selectClause,
                                    messageColumnSelected,
                                    distinct);

      if ((validity == AcceptedMessages) && (!messageColumnSelected))
        selectClause.clear();
    }
    else
    {
      // Must have at least 1 rejected message column
      //
      buildMessageQuerySelectClause(rejectedMessageQueryTables,
                                    StringList(),
                                    paramList,
                                    false,
                                    selectClause,
                                    messageColumnSelected,
                                    distinct);
    }

    if (selectClause.empty())
      throw SmartMet::Spine::Exception(BCP, "No applicable 'param' names given");
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Check given station id's exist
 */
// ----------------------------------------------------------------------

void Engine::validateStationIds(const Connection& connection,
                                const StationIdList& stationIdList,
                                bool debug) const
{
  try
  {
    if (stationIdList.empty())
      return;

    ostringstream selectFromWhereClause;

    size_t n = 0;

    for (auto stationId : stationIdList)
    {
      selectFromWhereClause << ((n == 0) ? "SELECT request_stations.station_id FROM (VALUES ("
                                         : "),(")
                            << stationId;
      n++;
    }

    selectFromWhereClause << ")) AS request_stations (station_id) LEFT JOIN avidb_stations ON "
                             "request_stations.station_id = avidb_stations.station_id "
                          << "WHERE avidb_stations.station_id IS NULL LIMIT 1";

    QueryData queryData;

    queryData.itsColumns.push_back(Column(Integer, "station_id"));

    executeQuery<QueryData>(connection, selectFromWhereClause.str(), debug, queryData);

    if (!queryData.itsValues.empty())
    {
      int stationId(boost::get<int>(&(queryData.itsValues["station_id"].front()))
                        ? *(boost::get<int>(&(queryData.itsValues["station_id"].front())))
                        : -1);
      throw SmartMet::Spine::Exception(BCP, "Unknown station id " + Fmi::to_string(stationId));
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Check given icao codes exist
 */
// ----------------------------------------------------------------------

void Engine::validateIcaos(const Connection& connection,
                           const StringList& icaoList,
                           bool debug) const
{
  try
  {
    if (icaoList.empty())
      return;

    ostringstream selectFromWhereClause;

    size_t n = 0;

    for (auto const& icao : icaoList)
    {
      selectFromWhereClause << ((n == 0)
                                    ? "SELECT request_icaos.icao_code FROM (VALUES (quote_literal('"
                                    : "')),(quote_literal('")
                            << Fmi::ascii_toupper_copy(icao);
      n++;
    }

    selectFromWhereClause
        << "'))) AS request_icaos (icao_code) LEFT JOIN avidb_stations ON "
           "BTRIM(request_icaos.icao_code,'''') = UPPER(avidb_stations.icao_code) "
        << "WHERE avidb_stations.icao_code IS NULL LIMIT 1";

    QueryData queryData;

    queryData.itsColumns.push_back(Column(String, "icao_code"));

    executeQuery<QueryData>(connection, selectFromWhereClause.str(), debug, queryData);

    if (!queryData.itsValues.empty())
    {
      string icaoCode(boost::get<std::string>(&(queryData.itsValues["icao_code"].front()))
                          ? *(boost::get<std::string>(&(queryData.itsValues["icao_code"].front())))
                          : "?");
      throw SmartMet::Spine::Exception(BCP, "Unknown icao code " + icaoCode);
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Check given country codes exist
 */
// ----------------------------------------------------------------------

void Engine::validateCountries(const Connection& connection,
                               const StringList& countryList,
                               bool debug) const
{
  try
  {
    if (countryList.empty())
      return;

    ostringstream selectFromWhereClause;

    size_t n = 0;

    for (auto const& country : countryList)
    {
      selectFromWhereClause
          << ((n == 0)
                  ? "WITH request_countries AS (SELECT country_code FROM (VALUES (quote_literal('"
                  : "')),(quote_literal('")
          << Fmi::ascii_toupper_copy(country);
      n++;
    }

    selectFromWhereClause
        << "'))) AS request_countries (country_code)) SELECT country_code FROM request_countries "
        << "WHERE NOT EXISTS (SELECT station_id FROM avidb_stations WHERE "
           "BTRIM(request_countries.country_code,'''') = UPPER(avidb_stations.country_code)) LIMIT "
           "1";

    QueryData queryData;

    queryData.itsColumns.push_back(Column(String, "country_code"));

    executeQuery<QueryData>(connection, selectFromWhereClause.str(), debug, queryData);

    if (!queryData.itsValues.empty())
    {
      string countryCode(
          boost::get<std::string>(&(queryData.itsValues["country_code"].front()))
              ? *(boost::get<std::string>(&(queryData.itsValues["country_code"].front())))
              : "?");
      throw SmartMet::Spine::Exception(BCP, "Unknown country code " + countryCode);
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Check given wkt's are valid and of supported type.
 * 		  Convert POINTs to latlons to support 'max # of nearest station' search.
 */
// ----------------------------------------------------------------------

void Engine::validateWKTs(const Connection& connection,
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

    size_t n = 0;

    selectFromWhereClause << "SELECT wkt,geomtype,isvalid,index,"
                          << "CASE geomtype WHEN 'ST_Point' THEN ST_Y(geom) ELSE 0 END AS lat,CASE "
                             "geomtype WHEN 'ST_Point' THEN ST_X(geom) ELSE 0 END AS lon "
                          << "FROM (SELECT wkt,ST_GeomFromText(BTRIM(wkt,''''),4326) as "
                             "geom,ST_GeometryType(ST_GeomFromText(BTRIM(wkt,''''),4326)) as "
                             "geomtype,"
                          << "CASE WHEN NOT ST_IsValid(ST_GeomFromText(BTRIM(wkt,''''),4326)) OR "
                          << "ST_GeometryType(ST_GeomFromText(BTRIM(wkt,''''),4326)) NOT IN "
                             "('ST_Point','ST_Polygon','ST_LineString') "
                          << "THEN 0 ELSE 1 END AS isvalid,index FROM (VALUES (quote_literal('";

    for (auto const& wkt : locationOptions.itsWKTs.itsWKTs)
    {
      selectFromWhereClause << ((n > 0) ? "),(quote_literal('" : "") << wkt << "')," << n;
      n++;
    }

    selectFromWhereClause
        << ")) AS request_wkts (wkt,index)) AS wkts ORDER BY isvalid,CASE geomtype "
           "WHEN 'ST_Point' THEN 0 ELSE 1 END,index";

    // If a single LINESTRING (route) is given, the stations (and their messages) will be ordered by
    // route segment index and
    // station's distance to the start of the segment. Otherwise icao code order is used

    bool checkIfRoute =
        (locationOptions.itsLonLats.empty() && locationOptions.itsStationIds.empty() &&
         locationOptions.itsIcaos.empty() && locationOptions.itsCountries.empty() &&
         locationOptions.itsPlaces.empty() && locationOptions.itsBBoxes.empty() &&
         (locationOptions.itsWKTs.itsWKTs.size() == 1));

    QueryData queryData;

    queryData.itsColumns.push_back(Column(String, "wkt"));
    queryData.itsColumns.push_back(Column(String, "geomtype"));
    queryData.itsColumns.push_back(Column(Integer, "isvalid"));
    queryData.itsColumns.push_back(Column(Integer, "index"));
    queryData.itsColumns.push_back(Column(Double, "lat"));
    queryData.itsColumns.push_back(Column(Double, "lon"));

    executeQuery<QueryData>(connection, selectFromWhereClause.str(), debug, queryData);

    if (queryData.itsValues["wkt"].size() != n)
      throw SmartMet::Spine::Exception(
          BCP, "validateWKTs: internal: wkt check query did not return as many rows as expected");

    bool isValid = ((boost::get<int>(&(queryData.itsValues["isvalid"].front()))
                         ? *(boost::get<int>(&(queryData.itsValues["isvalid"].front())))
                         : 0) == 1);

    if (!isValid)
    {
      string wkt(boost::get<std::string>(&(queryData.itsValues["wkt"].front()))
                     ? *(boost::get<std::string>(&(queryData.itsValues["wkt"].front())))
                     : "?");
      string geomType(boost::get<std::string>(&(queryData.itsValues["geomtype"].front()))
                          ? *(boost::get<std::string>(&(queryData.itsValues["geomtype"].front())))
                          : "?");

      if ((geomType == "ST_Point") || (geomType == "ST_Polygon") || (geomType == "ST_LineString"))
        throw SmartMet::Spine::Exception(BCP, "Invalid wkt " + wkt);

      throw SmartMet::Spine::Exception(
          BCP, "Unsupported wkt " + wkt + "; use POINT(s), POLYGON(s) or LINESTRING(s)");
    }

    // Convert POINTs to latlons to support 'max # of nearest station' search

    StringList::iterator itwkt = locationOptions.itsWKTs.itsWKTs.begin();
    string geomType;

    for (int wktIndex = 0, dataIndex = 0; (n > 0); n--, wktIndex++, dataIndex++)
    {
      geomType = (boost::get<std::string>(&(queryData.itsValues["geomtype"][dataIndex]))
                      ? *(boost::get<std::string>(&(queryData.itsValues["geomtype"][dataIndex])))
                      : "?");

      if (geomType != "ST_Point")
        break;

      int index = (boost::get<int>(&(queryData.itsValues["index"][dataIndex]))
                       ? *(boost::get<int>(&(queryData.itsValues["index"][dataIndex])))
                       : -1);

      if (index >= 0)
      {
        for (; ((wktIndex < index) && (itwkt != locationOptions.itsWKTs.itsWKTs.end())); wktIndex++)
          itwkt++;
      }

      if ((index < 0) || (itwkt == locationOptions.itsWKTs.itsWKTs.end()))
        throw SmartMet::Spine::Exception(BCP, "validateWKTs: internal: wkt index is invalid");

      double lat = (boost::get<double>(&(queryData.itsValues["lat"][dataIndex]))
                        ? *(boost::get<double>(&(queryData.itsValues["lat"][dataIndex])))
                        : 0);
      double lon = (boost::get<double>(&(queryData.itsValues["lon"][dataIndex]))
                        ? *(boost::get<double>(&(queryData.itsValues["lon"][dataIndex])))
                        : 0);

      locationOptions.itsLonLats.push_back(LonLat(lon, lat));

      itwkt = locationOptions.itsWKTs.itsWKTs.erase(itwkt);
    }

    // Set 'route' flag if the one and only wkt is a LINESTRING

    locationOptions.itsWKTs.isRoute = (checkIfRoute && (geomType == "ST_LineString"));
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
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
StationQueryData Engine::queryStations(const Connection& connection,
                                       QueryOptions& queryOptions) const
{
  try
  {
    // Validate requested parameters, station id's, icao codes, country codes and wkts

    auto const& paramList = queryOptions.itsParameters;
    auto& locationOptions = queryOptions.itsLocationOptions;

    validateParameters(paramList, Accepted, queryOptions.itsMessageColumnSelected);

    if (!locationOptions.itsStationIds.empty())
      validateStationIds(connection, locationOptions.itsStationIds, queryOptions.itsDebug);

    if (!locationOptions.itsIcaos.empty())
      validateIcaos(connection, locationOptions.itsIcaos, queryOptions.itsDebug);

    if (!locationOptions.itsCountries.empty())
      validateCountries(connection, locationOptions.itsCountries, queryOptions.itsDebug);

    if (!locationOptions.itsWKTs.itsWKTs.empty())
      validateWKTs(connection, locationOptions, queryOptions.itsDebug);

    // If any message column is requested, select only stationid and additionally distance/bearing
    // if
    // requested (all other requested station columns except
    // distance/bearing are selected by message query) and queryOptions.messageColumnSelected is set
    // instructing the caller to call queryMessages() with the returned station id's
    //
    // Build select column expressions
    //
    // distance is automatically selected if querying with coordinates and max # of nearest stations

    StationQueryData stationQueryData;
    bool selectStationListOnly = queryOptions.itsMessageColumnSelected;
    bool autoSelectDistance =
        ((!locationOptions.itsLonLats.empty()) && (locationOptions.itsNumberOfNearestStations > 0));
    string selectClause;

    stationQueryData.itsColumns = buildStationQuerySelectClause(
        paramList, selectStationListOnly, autoSelectDistance, selectClause);

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
                             queryOptions.itsDebug,
                             stationQueryData);

    if (!locationOptions.itsCountries.empty())
      queryStationsWithCountries(connection,
                                 locationOptions.itsCountries,
                                 selectClause,
                                 queryOptions.itsDebug,
                                 stationQueryData);

    if (!locationOptions.itsPlaces.empty())
      queryStationsWithPlaces(connection,
                              locationOptions.itsPlaces,
                              selectClause,
                              queryOptions.itsDebug,
                              stationQueryData);

    if (!locationOptions.itsWKTs.itsWKTs.empty())
      queryStationsWithWKTs(
          connection, locationOptions, selectClause, queryOptions.itsDebug, stationQueryData);

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
        stationQueryData.itsColumns.ascendingNumberOrder();

        for (ColumnList::iterator it = stationQueryData.itsColumns.begin();
             (it != stationQueryData.itsColumns.end());)
        {
          if (it->getSelection() == Automatic)
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}
//
// public api stub
//
StationQueryData Engine::queryStations(QueryOptions& queryOptions) const
{
  try
  {
    Connection connection(itsConfig->getHost(),
                          itsConfig->getPort(),
                          itsConfig->getUsername(),
                          itsConfig->getPassword(),
                          itsConfig->getDatabase(),
                          itsConfig->getEncoding());

    return queryStations(connection, queryOptions);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Check given message types are known
 */
// ----------------------------------------------------------------------

void Engine::validateMessageTypes(const Connection& connection,
                                  const StringList& messageTypeList,
                                  bool debug) const
{
  try
  {
    if (messageTypeList.empty())
      return;

    // Checking against configuration (not the database)

    MessageTypes::const_iterator it_begin = itsConfig->getMessageTypes().begin();
    MessageTypes::const_iterator it_end = itsConfig->getMessageTypes().end();

    for (auto const& msgType : messageTypeList)
      if (find(it_begin, it_end, msgType) == it_end)
        throw SmartMet::Spine::Exception(BCP, "Unknown message type " + msgType);

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
            throw SmartMet::Spine::Exception(BCP,"Unknown message type " + msgType);
    }
    */
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Get pointer to internal column definition for given
 *		  datetime column in avidb_messages table
 */
// ----------------------------------------------------------------------

const Column* Engine::getMessageTableTimeColumn(const string& timeColumn) const
{
  try
  {
    ColumnList columns;
    bool duplicate;

    auto queryColumn = getQueryColumn(messageQueryColumns, columns, timeColumn, duplicate);

    if ((!queryColumn) || (queryColumn->getType() != DateTime))
      throw SmartMet::Spine::Exception(
          BCP, string("Column '") + timeColumn + "' is not a datetime column in message table");

    return queryColumn;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
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
StationQueryData Engine::queryMessages(const Connection& connection,
                                       const StationIdList& stationIdList,
                                       const QueryOptions& queryOptions) const
{
  try
  {
    // Check # of stations and validate requested parameters and message types

    int maxStations =
        (queryOptions.itsMaxMessageStations >= 0 ? queryOptions.itsMaxMessageStations
                                                 : itsConfig->getMaxMessageStations());

    if ((maxStations > 0) && ((int)stationIdList.size() > maxStations))
      throw SmartMet::Spine::Exception(
          BCP,
          string("Max number of stations exceeded (") + Fmi::to_string(maxStations) + "/" +
              Fmi::to_string(stationIdList.size()) + "), limit the query");

    bool messageColumnSelected;
    const Column* timeRangeColumn = nullptr;

    validateParameters(queryOptions.itsParameters, AcceptedMessages, messageColumnSelected);

    if (!queryOptions.itsMessageTypes.empty())
      validateMessageTypes(connection, queryOptions.itsMessageTypes, queryOptions.itsDebug);

    // If querying messages created within time range, get the column to be used for time
    // restriction
    // (message_time by default, not settable currently)

    if (queryOptions.itsTimeOptions.itsObservationTime.empty() &&
        (!queryOptions.itsTimeOptions.itsQueryValidRangeMessages))
      timeRangeColumn =
          getMessageTableTimeColumn(queryOptions.itsTimeOptions.getMessageTableTimeRangeColumn());

    // Build select column expressions

    bool routeQuery = queryOptions.itsLocationOptions.itsWKTs.isRoute,
         distinct = queryOptions.itsDistinctMessages;
    string selectClause;

    TableMap tableMap = buildMessageQuerySelectClause(messageQueryTables,
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

    stationQueryData.itsColumns.ascendingNumberOrder();

    string withClause;

    if (queryOptions.itsLocationOptions.itsWKTs.isRoute)
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
    // range) the query is generated based on 'record_set' CTE
    // using message_time restriction and thus enabling use of an index. Otherwise an indexed time
    // column (queryOptions.itsTimeOptions.itsTimeRangeColumn)
    // is expected to be used for 'created within' query; the query is generated directly against
    // avidb_messages table

    if (!queryOptions.itsTimeOptions.itsObservationTime.empty() ||
        queryOptions.itsTimeOptions.itsQueryValidRangeMessages)
    {
      // Build WITH clause for 'record_set' table containg all valid messages for given time
      // instant/range.
      //
      // Note: By passing false instead of queryOptions.itsLocationOptions.itsWKTs.isRoute
      // record_set.station_id is limited by IN (StationIdList)
      // instead by subquery from request_stations table which makes the query slower
      // (request_stations CTE is generated only for route query)

      string recordSetWithClause;

      if (queryOptions.itsTimeOptions.itsObservationTime.empty())
        recordSetWithClause =
            buildRecordSetWithClause(false /*queryOptions.itsLocationOptions.itsWKTs.isRoute*/,
                                     stationIdList,
                                     itsConfig->getRecordSetStartTimeOffsetHours(),
                                     itsConfig->getRecordSetEndTimeOffsetHours(),
                                     queryOptions.itsTimeOptions.itsStartTime,
                                     queryOptions.itsTimeOptions.itsEndTime);
      else
        recordSetWithClause =
            buildRecordSetWithClause(false /*queryOptions.itsLocationOptions.itsWKTs.isRoute*/,
                                     stationIdList,
                                     itsConfig->getRecordSetStartTimeOffsetHours(),
                                     itsConfig->getRecordSetEndTimeOffsetHours(),
                                     queryOptions.itsTimeOptions.itsObservationTime);

      withClause += ((withClause.empty() ? "WITH " : ",") + recordSetWithClause);

      // Build WITH clause for 'message_validity' table containing configured message type specific
      // validity length for message types having
      // MessageValidTimeRange[Latest] or MessageTimeRange[Latest] restriction

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
          timeRangeTypes.push_back(ValidTimeRange);
          timeRangeTypes.push_back(ValidTimeRangeLatest);
          timeRangeTypes.push_back(CreationValidTimeRange);
          timeRangeTypes.push_back(CreationValidTimeRangeLatest);
          timeRangeTypes.push_back(MessageValidTimeRange);
          timeRangeTypes.push_back(MessageValidTimeRangeLatest);
          timeRangeTypes.push_back(MessageTimeRange);

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
      // messages
      // having starttime <= message_time < endtime

      if (!queryOptions.itsTimeOptions.itsObservationTime.empty())
      {
        // Build WITH clause for 'latest_messages' table
        //
        withClause += ("," + buildLatestMessagesWithClause(
                                 queryOptions.itsMessageTypes,
                                 itsConfig->getMessageTypes(),
                                 queryOptions.itsTimeOptions.itsObservationTime,
                                 queryOptions.itsFilterMETARs && itsConfig->getFilterFIMETARxxx(),
                                 itsConfig->getFilterFIMETARxxxExcludeIcaos()));

        // Add 'latest_messages' into tablemap
        //
        // Note: 'latest_messages' is used thru subquery because of poor performance if joined.

        auto& table = tableMap[latestMessagesTable.itsName];
        table.itsJoin = latestMessagesTable.itsJoin;
        table.subQuery = true;
      }
      else
        withClause += buildMessageTimeRangeMessagesWithClause(
            queryOptions.itsMessageTypes,
            itsConfig->getMessageTypes(),
            queryOptions.itsTimeOptions.itsStartTime,
            queryOptions.itsTimeOptions.itsEndTime,
            queryOptions.itsFilterMETARs && itsConfig->getFilterFIMETARxxx(),
            itsConfig->getFilterFIMETARxxxExcludeIcaos());

      withClause += " ";

      // Replace avidb_messages with record_set in the tablemap; record_set contains all (columns
      // of)
      // needed avidb_messages rows, and is used instead

      auto it = tableMap.find(messageTableName);

      if (it == tableMap.end())
        throw SmartMet::Spine::Exception(
            BCP, "queryMessages: internal: Unable to get avidb_messages table");

      auto recordSetTable = it->second;
      tableMap.erase(it);

      tableMap[recordSetTableName] = recordSetTable;
    }
    else if (queryOptions.itsLocationOptions.itsWKTs.isRoute)
    {
      withClause += " ";

      if (queryOptions.itsFilterMETARs && itsConfig->getFilterFIMETARxxx())
      {
        // Ensure station table is joined into main query for METAR filtering
        // (for nonroute query it's joined anyways for ordering the rows by icao code)
        //
        string messageTypeIn = buildMessageTypeInClause(
            queryOptions.itsMessageTypes, itsConfig->getMessageTypes(), list<TimeRangeType>());

        if (messageTypeIn.find("'METAR") != string::npos)
        {
          auto& table = tableMap[stationTableName];

          if (table.itsAlias.empty())
            table.itsAlias = stationTableAlias;

          if (table.itsJoin.empty())
            table.itsJoin = stationTableJoin;
        }
      }
    }

    // For time range query ensure tablemap contains message_types table for joining into main
    // query;
    // needed for message type or type specific time range restriction

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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}
//
// public api stub
//
StationQueryData Engine::queryMessages(const StationIdList& stationIdList,
                                       const QueryOptions& queryOptions) const
{
  try
  {
    Connection connection(itsConfig->getHost(),
                          itsConfig->getPort(),
                          itsConfig->getUsername(),
                          itsConfig->getPassword(),
                          itsConfig->getDatabase(),
                          itsConfig->getEncoding());

    return queryMessages(connection, stationIdList, queryOptions);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Join station data to another by station id
 */
// ----------------------------------------------------------------------

StationQueryData& Engine::joinStationAndMessageData(const StationQueryData& stationData,
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
        //		 distance and/or bearing, copy the values to 'message' data
        //
        if (messageData.itsValues.find(station.first) != messageData.itsValues.end())
        {
          if (hasDistance)
          {
            // Copy nonnull distance value to all message rows for the station
            //
            auto const& distanceValue =
                station.second.find(stationDistanceQueryColumn)->second.front();

            if (boost::get<double>(&distanceValue))
            {
              auto stationDistance = *(boost::get<double>(&distanceValue));

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

            if (boost::get<double>(&bearingValue))
            {
              auto stationBearing = *(boost::get<double>(&bearingValue));

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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query stations and/or accepted messages.
 *
 *		  This method serves for convenience, by executing station and message querys
 *		  and joining the data.
 */
// ----------------------------------------------------------------------

StationQueryData Engine::queryStationsAndMessages(QueryOptions& queryOptions) const
{
  try
  {
    if (queryOptions.itsValidity == Avi::Rejected)
      throw SmartMet::Spine::Exception(
          BCP,
          "queryStationsAndMessages() can't be used to query rejected messages; use "
          "queryRejectedMessages() instead");

    // Query stations

    Connection connection(itsConfig->getHost(),
                          itsConfig->getPort(),
                          itsConfig->getUsername(),
                          itsConfig->getPassword(),
                          itsConfig->getDatabase(),
                          itsConfig->getEncoding());

    StationQueryData stationData = queryStations(connection, queryOptions);

    // Query messages if any message column were requested

    if (queryOptions.itsMessageColumnSelected)
    {
      if (!stationData.itsStationIds.empty())
      {
        // Query messages and join station and message data to get distance and bearing values for
        // message data rows
        //
        StationQueryData messageData =
            queryMessages(connection, stationData.itsStationIds, queryOptions);

        return joinStationAndMessageData(stationData, messageData);
      }
    }

    return stationData;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Query rejected messages
 */
// ----------------------------------------------------------------------

QueryData Engine::queryRejectedMessages(const QueryOptions& queryOptions) const
{
  try
  {
    Connection connection(itsConfig->getHost(),
                          itsConfig->getPort(),
                          itsConfig->getUsername(),
                          itsConfig->getPassword(),
                          itsConfig->getDatabase(),
                          itsConfig->getEncoding());

    // Validate time options, parameters and message types

    if (!queryOptions.itsTimeOptions.itsObservationTime.empty())
      throw SmartMet::Spine::Exception(BCP, "Time range must be used to query rejected messages");

    bool messageColumnSelected;

    validateParameters(queryOptions.itsParameters, Rejected, messageColumnSelected);

    if (!queryOptions.itsMessageTypes.empty())
      validateMessageTypes(connection, queryOptions.itsMessageTypes, queryOptions.itsDebug);

    // Build select column expressions

    string selectClause;
    bool distinct = false;

    TableMap tableMap = buildMessageQuerySelectClause(rejectedMessageQueryTables,
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

    queryData.itsColumns.sort();

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
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet

// DYNAMIC MODULE CREATION TOOLS

extern "C" void* engine_class_creator(const char* theConfigFileName, void* /* user_data */)
{
  return new SmartMet::Engine::Avi::Engine(theConfigFileName);
}

extern "C" const char* engine_name()
{
  return "Avi";
}
// ======================================================================
