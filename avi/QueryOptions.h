#pragma once

#include "LocationOptions.h"
#include "TimeOptions.h"

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
// Type for passing query options

typedef enum
{
  Accepted,
  Rejected,
  AcceptedMessages
} Validity;

struct QueryOptions
{
  QueryOptions()
      : itsMessageTypes(),
        itsParameters(),
        itsLocationOptions(),
        itsTimeOptions(),
        itsValidity(Accepted),
        itsMaxMessageStations(-1),
        itsMaxMessageRows(-1),
        itsDistinctMessages(true),
        itsFilterMETARs(true),
        itsDebug(false)
  {
  }

  StringList itsMessageTypes;
  StringList itsParameters;
  LocationOptions itsLocationOptions;
  TimeOptions itsTimeOptions;
  Validity itsValidity;  // Whether to select accepted or rejected messages

  bool itsMessageColumnSelected;  // Whether any avidb_messages column is requested or not

  int itsMaxMessageStations;  // if 0, unlimited; if < 0, engine rules
  int itsMaxMessageRows;      // if 0, unlimited; if < 0, engine rules

  bool itsDistinctMessages;  // Whether to skip duplicate messages or not

  bool itsFilterMETARs;  // Whether to filter (finnish) METARs (LIKE 'METAR%', if enabled by
                         // engine's configuration) or not

  bool itsDebug;  // Whether to write generated sql queries to stderr or not
};

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
