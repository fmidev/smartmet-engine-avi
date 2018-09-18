#include "BuildRejectedMessageQuery.h"

#include "ConstantValues.h"

#include <macgyver/StringConversion.h>
#include <spine/Exception.h>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
void BuildRejectedMessageQuery::fromWhereOrderByClause(int maxMessageRows,
                                                       const QueryOptions& queryOptions,
                                                       const TableMap& tableMap,
                                                       std::ostringstream& fromWhereOrderByClause)
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

    std::string whereOrAnd((n == 0) ? " WHERE " : " AND ");

    if (!queryOptions.itsMessageTypes.empty())
    {
      std::string messageTypeIn = whereOrAnd + "UPPER(" + messageTypeTableAlias + ".type) IN ('";
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
