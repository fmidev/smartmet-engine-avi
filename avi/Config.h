// ======================================================================

#pragma once

#include <spine/ConfigBase.h>
#include <algorithm>
#include <list>

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

typedef enum
{
  NoScope = 0,
  StationScope,
  FIRScope,
  GlobalScope
} MessageScope;

class MessageType
{
 public:
  MessageType()
  {
    itsScope = StationScope;
    itsTimeRangeType = NullTimeRange;
    itsValidityHours = 0;
    itsLatestMessageOnly = false;
    itsQueryRestrictionStartMinute = itsQueryRestrictionEndMinute = 0;
  }

  bool operator==(const std::string &theMessageType) const
  {
    return (std::find(itsTypes.begin(), itsTypes.end(), theMessageType) != itsTypes.end());
  }

  void addType(const std::string &theType) { itsTypes.push_back(theType); }
  void setScope(MessageScope theScope) { itsScope = theScope; }
  void setTimeRangeType(TimeRangeType theTimeRangeType) { itsTimeRangeType = theTimeRangeType; }
  void setValidityHours(unsigned int theValidityHours) { itsValidityHours = theValidityHours; }
  void setLatestMessageOnly(bool theLatestMessageOnly)
  {
    itsLatestMessageOnly = theLatestMessageOnly;
  }
  void addMessirPattern(const std::string &theMessirPattern)
  {
    itsMessirPatterns.push_back(theMessirPattern);
  }
  void setQueryRestrictionHours(const std::string &hours)
  {
    itsQueryRestrictionHours = hours;
  }
  void addQueryRestrictionIcaoPattern(const std::string &theIcaoPattern)
  {
    itsQueryRestrictionIcaoPatterns.push_back(theIcaoPattern);
  }
  void addQueryRestrictionCountryCode(const std::string &theCountryCode)
  {
    itsQueryRestrictionCountryCodes.push_back(theCountryCode);
  }
  void setQueryRestrictionStartMinute(int minute)
  {
    itsQueryRestrictionStartMinute = minute;
  }
  void setQueryRestrictionEndMinute(int minute)
  {
    itsQueryRestrictionEndMinute = minute;
  }

  const std::list<std::string> &getMessageTypes() const { return itsTypes; }
  MessageScope getScope() const { return itsScope; }
  TimeRangeType getTimeRangeType() const { return itsTimeRangeType; }
  bool hasValidityHours() const
  {
    return ((itsTimeRangeType == MessageValidTimeRange) ||
            (itsTimeRangeType == MessageValidTimeRangeLatest) ||
            (itsTimeRangeType == MessageTimeRange) || (itsTimeRangeType == MessageTimeRangeLatest));
  }
  unsigned int getValidityHours() const { return itsValidityHours; }
  bool getLatestMessageOnly() const { return itsLatestMessageOnly; }
  const std::list<std::string> &getMessirPatterns() const { return itsMessirPatterns; }
  const std::string &getQueryRestrictionHours() const { return itsQueryRestrictionHours; }
  const std::list<std::string> &getQueryRestrictionIcaoPatterns() const
  {
    return itsQueryRestrictionIcaoPatterns;
  }
  const std::list<std::string> &getQueryRestrictionCountryCodes() const
  {
    return itsQueryRestrictionCountryCodes;
  }
  int getQueryRestrictionStartMinute() const { return itsQueryRestrictionStartMinute; }
  int getQueryRestrictionEndMinute() const { return itsQueryRestrictionEndMinute; }

 private:
  std::list<std::string> itsTypes;
  MessageScope itsScope;
  TimeRangeType itsTimeRangeType;
  unsigned int itsValidityHours;
  bool itsLatestMessageOnly;
  std::list<std::string> itsMessirPatterns;  // Querying latest messages grouped additionally by
                                             // messir_heading (e.g. GAFOR; FBFI41..., FBFI42...,
                                             // FBFI43...)
  std::string itsQueryRestrictionHours;	     // TAF query restriction hours (e.g. 2,5,8,...)
  std::list<std::string> itsQueryRestrictionIcaoPatterns;  // ... icao patterns
  std::list<std::string> itsQueryRestrictionCountryCodes;  // ... country codes
  int itsQueryRestrictionStartMinute;                      // ... starting minute (e.g. 20)
  int itsQueryRestrictionEndMinute;                        // ... ending minute (e.g. 40)
};

typedef std::list<MessageType> MessageTypes;

class Config : public SmartMet::Spine::ConfigBase
{
 public:
  Config(const std::string &theFileName);
  Config() = delete;
  Config(const Config &) = delete;
  Config &operator=(const Config &) = delete;

  const std::string &getHost() const { return itsHost; }
  int getPort() const { return itsPort; }
  const std::string &getDatabase() const { return itsDatabase; }
  const std::string &getUsername() const { return itsUsername; }
  const std::string &getPassword() const { return itsPassword; }
  const std::string &getEncoding() const { return itsEncoding; }
  int getMaxMessageStations() const { return itsMaxMessageStations; }
  int getMaxMessageRows() const { return itsMaxMessages; }
  int getRecordSetStartTimeOffsetHours() const { return itsRecordSetStartTimeOffsetHours; }
  int getRecordSetEndTimeOffsetHours() const { return itsRecordSetEndTimeOffsetHours; }
  bool getFilterFIMETARxxx() const { return itsFilterFIMETARxxx; }
  const std::list<std::string> &getFilterFIMETARxxxExcludeIcaos() const
  {
    return itsFilterFIMETARxxxExcludeIcaos;
  }

  const MessageTypes &getMessageTypes() const { return itsMessageTypes; }

 private:
  std::string itsHost;
  int itsPort;
  std::string itsDatabase;
  std::string itsUsername;
  std::string itsPassword;
  std::string itsEncoding;

  int itsMaxMessageStations;  // if config/query value not given or <= 0, unlimited
  int itsMaxMessages;         // if config/query value not given or <= 0, unlimited

  // Database has no indexes for message table's valid_from and valid_to columns used to restrict
  // query for some message types.
  //
  // For better throughput message_time column (indexed, primary/partition key) is used to first
  // load all possible rows needed by the
  // query into a 'record_set' (CTE; WITH clause / runtime query table) using configurable offsets
  // expanding the message_time range
  // to include messages that can be valid at/within the time instant/range requested.
  //
  // Message type specific time restrictions/queries are then applied to the record_set.

  unsigned int itsRecordSetStartTimeOffsetHours;  // record_set's starting message_time as # of
                                                  // hours backwards from observation time / time
                                                  // range start time
  unsigned int itsRecordSetEndTimeOffsetHours;    // record_set's ending message_time as # of hours
                                                  // forwards from observation time / time range end
                                                  // time

  MessageTypes itsMessageTypes;

  // Currently METARs from some finnish stations are stored twice into the database, with and
  // without "METAR" in the beginning of message.
  // Only messages LIKE "METAR%" are returned by default if configuration has setting
  // "message.filter_FI_METARxxx.filter" with value true.
  // Stations can be excluded from filtering by listing their icao code in configuration setting
  // "message.filter_FI_METARxxx.excludeicaos"

  bool itsFilterFIMETARxxx;
  std::list<std::string> itsFilterFIMETARxxxExcludeIcaos;
};  // class Config

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet

// ======================================================================
