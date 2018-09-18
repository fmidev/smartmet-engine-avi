#pragma once

#include "TimeRangeType.h"

#include <list>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
class MessageType
{
 public:
  MessageType();

  bool operator==(const std::string &theMessageType) const;
  void addType(const std::string &theType);
  void setTimeRangeType(TimeRangeType theTimeRangeType);
  void setValidityHours(unsigned int theValidityHours);
  void setLatestMessageOnly(bool theLatestMessageOnly);

  void addMessirPattern(const std::string &theMessirPattern);

  const std::list<std::string> &getMessageTypes() const;
  TimeRangeType getTimeRangeType() const;
  unsigned int getValidityHours() const;
  bool getLatestMessageOnly() const;
  const std::list<std::string> &getMessirPatterns() const;

  bool hasValidityHours() const;

 private:
  std::list<std::string> itsTypes;
  TimeRangeType itsTimeRangeType;
  unsigned int itsValidityHours;
  bool itsLatestMessageOnly;
  std::list<std::string> itsMessirPatterns;  // Querying latest messages grouped additionally by
                                             // messir_heading (e.g. GAFOR; FBFI41..., FBFI42...,
                                             // FBFI43...)
};

using MessageTypes = std::list<MessageType>;
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
