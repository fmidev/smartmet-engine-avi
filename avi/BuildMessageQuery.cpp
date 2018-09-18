#include "BuildMessageQuery.h"

#include "BuildMessageType.h"
#include "ConstantValues.h"
#include "Utils.h"

#include <spine/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
void BuildMessageQuery::whereStationIdInClause(const StationIdList& stationIdList,
                                               std::ostringstream& whereClause)
{
  try
  {
    // { WHERE | AND } me.station_id IN (stationIdList)

    std::string whereStationIdIn = (std::string(whereClause.str().empty() ? " WHERE " : " AND ") +
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void BuildMessageQuery::fromWhereOrderByClause(int maxMessageRows,
                                               const StationIdList& stationIdList,
                                               const QueryOptions& queryOptions,
                                               const TableMap& tableMap,
                                               const Config& config,
                                               const Column* timeRangeColumn,
                                               bool distinct,
                                               std::ostringstream& fromWhereOrderByClause)
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

        std::list<TimeRangeType> timeRangeTypes;
        timeRangeTypes.push_back(ValidTimeRange);
        timeRangeTypes.push_back(ValidTimeRangeLatest);

        std::string messageTypeIn = BuildMessageType::inClause(
            queryOptions.itsMessageTypes, knownMessageTypes, timeRangeTypes);
        std::string emptyOrOr = "";

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

        messageTypeIn = BuildMessageType::inClause(
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

        messageTypeIn = BuildMessageType::inClause(
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

        messageTypeIn = BuildMessageType::inClause(
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

        messageTypeIn = BuildMessageType::inClause(
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
          throw Spine::Exception(
              BCP, "buildMessageQueryFromWhereOrderByClause(): internal: time column is NULL");

        BuildMessageQuery::whereStationIdInClause(stationIdList, fromWhereOrderByClause);
        std::string messageTypeIn = BuildMessageType::inClause(
            queryOptions.itsMessageTypes, knownMessageTypes, std::list<TimeRangeType>());

        fromWhereOrderByClause << " AND " << messageTypeIn << " AND " << messageTableAlias << "."
                               << timeRangeColumn->getTableColumnName()
                               << " >= " << queryOptions.itsTimeOptions.itsStartTime << " AND "
                               << messageTableAlias << "." << timeRangeColumn->getTableColumnName()
                               << " < " << queryOptions.itsTimeOptions.itsEndTime;

        if (queryOptions.itsFilterMETARs && config.getFilterFIMETARxxx() &&
            (messageTypeIn.find("'METAR") != std::string::npos))
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
                                   << Utils::getStringList(filterFIMETARxxxExcludeIcaos) << ")";

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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
