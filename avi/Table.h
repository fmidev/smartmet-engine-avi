#pragma once

#include "ColumnList.h"

#include <map>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
struct QueryTable
{
  std::string itsName;
  std::string itsAlias;
  ColumnTable itsColumns;
  std::string itsJoin;
};

struct Table
{
  Table()
  {
    subQuery = false;
    leftOuter = false;
  }

  std::string itsAlias;
  ColumnList itsSelectedColumns;
  std::string itsJoin;

  // If set, generating only join condition (not FROM clause) for the table.
  //
  // Is only/always set for latest_messages; avidb_messages.message_id IN (SELECT message_id FROM
  // latest_messages)
  //
  bool subQuery;

  // If set, generating avidb_rejected_messages LEFT OUTER join for the table
  // (avidb_rejected_messages table's foreign key columns for route and message type are nullable).
  //
  // Is always set for avidb_messages_routes table; avidb_rejected_messages LEFT OUTER JOIN
  // avidb_messages_routes ON (route_id)
  // Is set for avidb_messages_types table if no message type restriction; avidb_rejected_messages
  // LEFT OUTER JOIN avidb_messages_types ON (type_id)
  //
  bool leftOuter;
};

typedef std::map<std::string, Table> TableMap;

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
