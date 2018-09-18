#include "BuildLatestMessages.h"

#include "BuildMessageType.h"
#include "BuildMessirHeading.h"
#include "ConstantValues.h"
#include "Utils.h"

#include <spine/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
std::string BuildLatestMessages::withClause(
    const StringList& messageTypes,
    const MessageTypes& knownMessageTypes,
    const std::string& observationTime,
    bool filterFIMETARxxx,
    const std::list<std::string>& filterFIMETARxxxExcludeIcaos)
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

    std::ostringstream withClause;
    std::string unionOrEmpty = "";

    withClause << latestMessagesTable.itsName << " AS (";

    std::string messageTypeIn =
        BuildMessageType::inClause(messageTypes, knownMessageTypes, ValidTimeRangeLatest);

    if (!messageTypeIn.empty())
    {
      // Depending on configuration the latest message for group(s) of types might be returned
      //
      std::string messageTypeGroupByExpr =
          BuildMessageType::groupByExpr(messageTypes, knownMessageTypes, ValidTimeRangeLatest);

      // Depending on configuration messir_heading LIKE pattern(s) might be used for additional
      // grouping

      std::string messirHeadingGroupByExpr =
          BuildMessirHeading::groupByExpr(messageTypes, knownMessageTypes, ValidTimeRangeLatest);

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
        BuildMessageType::inClause(messageTypes, knownMessageTypes, MessageValidTimeRangeLatest);

    if (!messageTypeIn.empty())
    {
      std::string messirHeadingGroupByExpr =
          BuildMessirHeading::groupByExpr(messageTypes, knownMessageTypes, MessageTimeRangeLatest);

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
        BuildMessageType::inClause(messageTypes, knownMessageTypes, MessageTimeRangeLatest);

    if (!messageTypeIn.empty())
    {
      std::string messageTypeGroupByExpr =
          BuildMessageType::groupByExpr(messageTypes, knownMessageTypes, MessageTimeRangeLatest);
      std::string messirHeadingGroupByExpr =
          BuildMessirHeading::groupByExpr(messageTypes, knownMessageTypes, MessageTimeRangeLatest);
      std::string whereOrAnd = " WHERE ";

      withClause << unionOrEmpty << "SELECT MAX(message_id) AS message_id FROM record_set "
                 << messageTableAlias << ",avidb_message_types mt"
                 << "," << messageValidityTable.itsName << " " << messageValidityTableAlias;

      if (filterFIMETARxxx && (messageTypeIn.find("'METAR") != std::string::npos))
      {
        withClause << "," << stationTableName << " " << stationTableAlias << " WHERE "
                   << stationTableJoin << " AND ((st.country_code != 'FI' OR mt.type != 'METAR' OR "
                   << messageTableAlias << ".message LIKE 'METAR%')";

        if (!filterFIMETARxxxExcludeIcaos.empty())
          withClause << " OR st.icao_code IN ("
                     << Utils::getStringList(filterFIMETARxxxExcludeIcaos) << ")";

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
        BuildMessageType::inClause(messageTypes, knownMessageTypes, CreationValidTimeRangeLatest);

    if (!messageTypeIn.empty())
    {
      std::string messirHeadingGroupByExpr = BuildMessirHeading::groupByExpr(
          messageTypes, knownMessageTypes, CreationValidTimeRangeLatest);

      withClause << unionOrEmpty << "SELECT MAX(message_id) AS message_id FROM record_set "
                 << messageTableAlias << ",avidb_message_types mt"
                 << " WHERE " << messageTypeTableJoin << " AND " << messageTypeIn << " AND "
                 << observationTime << " BETWEEN " << messageTableAlias << ".created AND "
                 << messageTableAlias << ".valid_to"
                 << " GROUP BY " << messageTableAlias << ".station_id" << messirHeadingGroupByExpr;

      unionOrEmpty = " UNION ALL ";
    }

    messageTypeIn = BuildMessageType::inClause(messageTypes, knownMessageTypes, ValidTimeRange);

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

    messageTypeIn = BuildMessageType::inClause(messageTypes, knownMessageTypes, MessageTimeRange);

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
        BuildMessageType::inClause(messageTypes, knownMessageTypes, CreationValidTimeRange);

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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
