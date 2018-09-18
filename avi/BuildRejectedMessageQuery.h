#pragma once

#include "QueryOptions.h"
#include "Table.h"

#include <sstream>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
namespace BuildRejectedMessageQuery
{
/*!
 * \brief Build from, where and order by clause with given message types,
 *		  message tables and time range for querying rejected messages
 */
void fromWhereOrderByClause(int maxMessageRows,
                            const QueryOptions& queryOptions,
                            const TableMap& tableMap,
                            std::ostringstream& fromWhereOrderByClause);

}  // namespace BuildRejectedMessageQuery
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
