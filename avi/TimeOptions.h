#pragma once

#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
#define messageTimeQueryColumn "messagetime"

// Type for passing time related options

struct TimeOptions
{
  TimeOptions(bool theQueryValidRangeMessages = true,
              std::string theMessageTableTimeRangeColumn = messageTimeQueryColumn)
  {
    itsQueryValidRangeMessages = theQueryValidRangeMessages;
    itsMessageTableTimeRangeColumn = theMessageTableTimeRangeColumn;
  }

  std::string itsObservationTime;   // Observation time (defaults to current time)
  std::string itsStartTime;         // Time range start time
  std::string itsEndTime;           // Time range end time
  std::string itsTimeFormat;        // Fmi::TimeFormatter type; iso, timestamp, sql, xml or epoch
  std::string itsTimeZone;          // tz for localtime output
  bool itsQueryValidRangeMessages;  // Whether to query valid accepted messages or accepted messages
                                    // created within time range

  const std::string &getMessageTableTimeRangeColumn() const
  {
    return itsMessageTableTimeRangeColumn;
  }

 private:
  std::string itsMessageTableTimeRangeColumn;  // Column for querying accepted messages created
                                               // within time range
};

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
