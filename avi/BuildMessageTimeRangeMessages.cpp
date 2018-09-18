#include "BuildMessageTimeRangeMessages.h"

#include "BuildMessageType.h"
#include "ConstantValues.h"
#include "Utils.h"

#include <spine/Exception.h>
#include <sstream>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
std::string BuildMessageTimeRangeMessages::withClause(
    const StringList& messageTypes,
    const MessageTypes& knownMessageTypes,
    const std::string& startTime,
    const std::string& endTime,
    bool filterFIMETARxxx,
    const std::list<std::string>& filterFIMETARxxxExcludeIcaos)
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

    std::string messageTypeIn =
        BuildMessageType::inClause(messageTypes, knownMessageTypes, MessageTimeRangeLatest);

    if (messageTypeIn.empty())
      return "";

    std::ostringstream withClause;
    std::string whereOrAnd = " WHERE ";

    withClause << "," << messageTimeRangeLatestMessagesTableName << " AS ("
               << "SELECT me.message_id "
               << "FROM record_set me,avidb_message_types mt";

    if (filterFIMETARxxx && (messageTypeIn.find("'METAR") != std::string::npos))
    {
      withClause << "," << stationTableName << " " << stationTableAlias << " WHERE "
                 << stationTableJoin << " AND ((st.country_code != 'FI' OR mt.type != 'METAR' OR "
                 << messageTableAlias << ".message LIKE 'METAR%')";

      if (!filterFIMETARxxxExcludeIcaos.empty())
        withClause << " OR st.icao_code IN (" << Utils::getStringList(filterFIMETARxxxExcludeIcaos)
                   << ")";

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
        withClause << " OR st.icao_code IN (" << Utils::getStringList(filterFIMETARxxxExcludeIcaos)
                   << ")";

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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
