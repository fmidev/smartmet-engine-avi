#pragma once

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
typedef enum
{
  NullTimeRange = 0,
  ValidTimeRange,
  ValidTimeRangeLatest,
  MessageValidTimeRange,
  MessageValidTimeRangeLatest,  // e.g. TAF; if valid from/to times are NULL (NIL messages), query
                                // like MessageTimeRange
  MessageTimeRange,
  MessageTimeRangeLatest,
  CreationValidTimeRange,
  CreationValidTimeRangeLatest
} TimeRangeType;
}
}  // namespace Engine
}  // namespace SmartMet
