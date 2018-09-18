#pragma once

#include "LocationOptions.h"
#include "MessageType.h"

#include <list>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
namespace BuildLatestMessages
{
/*!
 * \brief Build 'latest_messages' table (WITH clause) for querying latest
 *		  accepted messages for given observation time
 */
std::string withClause(const StringList& messageTypes,
                       const MessageTypes& knownMessageTypes,
                       const std::string& observationTime,
                       bool filterFIMETARxxx,
                       const std::list<std::string>& filterFIMETARxxxExcludeIcaos);
}  // namespace BuildLatestMessages
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
