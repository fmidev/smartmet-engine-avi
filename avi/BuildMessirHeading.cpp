#include "BuildMessirHeading.h"

#include "ConstantValues.h"

#include <spine/Exception.h>
#include <list>
#include <sstream>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
std::string BuildMessirHeading::groupByExpr(const StringList& messageTypeList,
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

    std::ostringstream groupBy;
    std::string whenMessageTypeIs =
        std::string(" WHEN UPPER(") + messageTypeTableAlias + ".type) = '";
    std::string whenMessirHeadingLike =
        std::string(" WHEN UPPER(") + messageTableAlias + ".messir_heading) LIKE '";
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

            for (std::list<std::string>::const_iterator it = messirPatterns.begin();
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

                for (std::list<std::string>::const_iterator it = messirPatterns.begin();
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
