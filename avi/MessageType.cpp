#include "MessageType.h"

#include <spine/Exception.h>
#include <algorithm>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
MessageType::MessageType()
{
  itsTimeRangeType = NullTimeRange;
  itsValidityHours = 0;
  itsLatestMessageOnly = false;
}

bool MessageType::operator==(const std::string &theMessageType) const
{
  try
  {
    return (std::find(itsTypes.begin(), itsTypes.end(), theMessageType) != itsTypes.end());
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "A message type find failed.");
  }
}

void MessageType::addType(const std::string &theType)
{
  try
  {
    itsTypes.push_back(theType);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "A message type add failed.");
  }
}
void MessageType::setTimeRangeType(TimeRangeType theTimeRangeType)
{
  try
  {
    itsTimeRangeType = theTimeRangeType;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "A time range set failed.");
  }
}
void MessageType::setValidityHours(unsigned int theValidityHours)
{
  try
  {
    itsValidityHours = theValidityHours;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "A validity hours set failed.");
  }
}
void MessageType::setLatestMessageOnly(bool theLatestMessageOnly)
{
  try
  {
    itsLatestMessageOnly = theLatestMessageOnly;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Latest messages only flag set failed.");
  }
}
void MessageType::addMessirPattern(const std::string &theMessirPattern)
{
  try
  {
    itsMessirPatterns.push_back(theMessirPattern);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "A messir pattern add failed.");
  }
}

const std::list<std::string> &MessageType::getMessageTypes() const
{
  try
  {
    return itsTypes;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Message types get failed.");
  }
}
TimeRangeType MessageType::getTimeRangeType() const
{
  try
  {
    return itsTimeRangeType;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Time range type get failed.");
  }
}
bool MessageType::hasValidityHours() const
{
  try
  {
    return ((itsTimeRangeType == MessageValidTimeRange) ||
            (itsTimeRangeType == MessageValidTimeRangeLatest) ||
            (itsTimeRangeType == MessageTimeRange) || (itsTimeRangeType == MessageTimeRangeLatest));
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Validity hours existence test failed.");
  }
}
unsigned int MessageType::getValidityHours() const
{
  try
  {
    return itsValidityHours;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Validity hours get failed.");
  }
}
bool MessageType::getLatestMessageOnly() const
{
  try
  {
    return itsLatestMessageOnly;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Latest messages only flag get failed.");
  }
}
const std::list<std::string> &MessageType::getMessirPatterns() const
{
  try
  {
    return itsMessirPatterns;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Messir patterns get failed.");
  }
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
