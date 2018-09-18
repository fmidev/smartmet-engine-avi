#include "BuildMessageType.h"

#include "ConstantValues.h"

#include <spine/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
std::string BuildMessageType::groupByExpr(const StringList& messageTypeList,
                                          const MessageTypes& knownMessageTypes,
                                          TimeRangeType timeRangeType)
{
  try
  {
    // [CASE mt.type WHEN type1 OR type2 [ .. ] THEN type1 [ WHEN .. ] ELSE ] mt.type [ END ]

    std::ostringstream groupBy;
    std::string whenMessageTypeIn =
        std::string(" WHEN UPPER(") + messageTypeTableAlias + ".type) IN ('";
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

            for (std::list<std::string>::const_iterator it = knownTypes.begin();
                 (it != knownTypes.end());
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
                std::list<std::string>::const_iterator it = knownTypes.begin();

                for (; (it != knownTypes.end()); it++)
                  if (find(messageTypeList.begin(), messageTypeList.end(), *it) ==
                      messageTypeList.end())
                    break;

                if (it == knownTypes.end())
                {
                  size_t nn = 0;

                  for (std::list<std::string>::const_iterator it = knownTypes.begin();
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

    return ((n > 0) ? groupBy.str() : (std::string(messageTypeTableAlias) + ".type"));
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

std::string BuildMessageType::inClause(const StringList& messageTypeList,
                                       const MessageTypes& knownMessageTypes,
                                       std::list<TimeRangeType> timeRangeTypes)
{
  try
  {
    // mt.type IN (messageTypeList)

    std::ostringstream whereClause;
    std::string messageTypeIn = (std::string("UPPER(") + messageTypeTableAlias + ".type) IN ('");
    size_t n = 0;
    bool getAll = timeRangeTypes.empty();

    if (messageTypeList.empty())
    {
      // Get all message types or all types matching timeRangeType(s)
      //
      for (auto const& knownType : knownMessageTypes)
        if (getAll || (std::find(timeRangeTypes.begin(),
                                 timeRangeTypes.end(),
                                 knownType.getTimeRangeType()) != timeRangeTypes.end()))
        {
          auto const& knownTypes = knownType.getMessageTypes();

          for (std::list<std::string>::const_iterator it = knownTypes.begin();
               (it != knownTypes.end());
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
              (getAll || (std::find(timeRangeTypes.begin(),
                                    timeRangeTypes.end(),
                                    knownType.getTimeRangeType()) != timeRangeTypes.end())))
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

std::string BuildMessageType::inClause(const StringList& messageTypeList,
                                       const MessageTypes& knownMessageTypes,
                                       TimeRangeType timeRangeType)
{
  try
  {
    std::list<TimeRangeType> timeRangeTypes;
    timeRangeTypes.push_back(timeRangeType);

    return inClause(messageTypeList, knownMessageTypes, timeRangeTypes);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

std::string BuildMessageType::validityWithClause(const StringList messageTypeList,
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

    std::ostringstream withClause;

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

          for (std::list<std::string>::const_iterator it = knownTypes.begin();
               (it != knownTypes.end());
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
