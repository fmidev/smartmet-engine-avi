// ======================================================================

#pragma once

#include <spine/SmartMetEngine.h>
#include <timeseries/TimeSeries.h>
#include <list>
#include <map>
#include <pqxx/result>
#include <utility>

#define stationIdQueryColumn "stationid"
#define messageQueryColumn "message"
#define messageTimeQueryColumn "messagetime"

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
// Types for passing location options

struct BBox
{
  BBox(double theWest, double theEast, double theSouth, double theNorth)
      : itsWest(theWest), itsEast(theEast), itsSouth(theSouth), itsNorth(theNorth)
  {
  }

  double getXMin() const { return itsEast; }
  double getYMin() const { return itsSouth; }
  double getXMax() const { return itsWest; }
  double getYMax() const { return itsNorth; }

  double itsWest;
  double itsEast;
  double itsSouth;
  double itsNorth;
};

struct LonLat
{
  LonLat(double theLon, double theLat) : itsLon(theLon), itsLat(theLat) {}
  double itsLon;
  double itsLat;
};

using StationIdType = long;
using StationIdList = std::list<StationIdType>;
using StringList = std::list<std::string>;
using BBoxList = std::list<BBox>;
using LonLatList = std::list<LonLat>;

struct WKTs
{
  StringList itsWKTs;
  bool isRoute;
};

using LocationOptions = struct
{
  StationIdList itsStationIds;
  StringList itsIcaos;
  BBoxList itsBBoxes;
  LonLatList itsLonLats;
  WKTs itsWKTs;
  StringList itsPlaces;
  StringList itsCountries;
  double itsMaxDistance;
  unsigned int itsNumberOfNearestStations;
  // BRAINSTORM-3288, BRAINSTORM-3300
  //
  // Country and icao code filters to include/exclude stations
  //
  StringList itsIncludeCountryFilters;
  StringList itsIncludeIcaoFilters;
  StringList itsExcludeIcaoFilters;
};

// Type for passing time related options

struct TimeOptions
{
  TimeOptions(bool theQueryValidRangeMessages = true,
              std::string theMessageTableTimeRangeColumn = messageTimeQueryColumn)
      : itsQueryValidRangeMessages(theQueryValidRangeMessages),
        itsMessageTableTimeRangeColumn(std::move(theMessageTableTimeRangeColumn))
  {
  }

  std::string itsObservationTime;     // Observation time (defaults to current time)
  std::string itsMessageCreatedTime;  // Message creation time (defaults to observation time)
  std::string itsStartTime;           // Time range start time
  std::string itsEndTime;             // Time range end time
  std::string itsTimeFormat;          // Fmi::TimeFormatter type; iso, timestamp, sql, xml or epoch
  std::string itsTimeZone;            // tz for localtime output
  bool itsQueryValidRangeMessages = true;  // Whether to query valid accepted messages or accepted
                                           // messages created within time range
  // BRAINSTORM-3301
  //
  // If false, do not check/filter if messages were created after the given messagetime and
  // do not apply message query time restrictions (used for TAFs)
  //
  bool itsMessageTimeChecks = true;

  const std::string &getMessageTableTimeRangeColumn() const
  {
    return itsMessageTableTimeRangeColumn;
  }

 private:
  std::string itsMessageTableTimeRangeColumn;  // Column for querying accepted messages created
                                               // within time range
};

// Type for passing query options

enum class Validity
{
  Accepted,
  Rejected,
  AcceptedMessages
};

struct QueryOptions
{
  QueryOptions() : itsMessageFormat("TAC") {}

  std::string itsMessageFormat;  // TAC, IWXXM
  StringList itsMessageTypes;
  StringList itsParameters;
  LocationOptions itsLocationOptions;
  mutable TimeOptions itsTimeOptions;
  Validity itsValidity{};  // Whether to select accepted or rejected messages

  bool itsMessageColumnSelected = false;  // Whether any avidb_messages column is requested or not
  int itsMaxMessageStations = 0;          // if 0, unlimited; if < 0, engine rules
  int itsMaxMessageRows = 0;              // if 0, unlimited; if < 0, engine rules
  bool itsDistinctMessages = true;        // Whether to skip duplicate messages or not
  bool itsFilterMETARs = true;  // Whether to filter (finnish) METARs (LIKE 'METAR%', if enabled by
                                // engine's configuration) or not
  bool itsExcludeSPECIs = false;
  // Whether to exclude (finnish) SPECIs (if enabled with request parameter)
  bool itsDebug = false;  // Whether to write generated sql queries to stderr or not
};

// Types for building query

enum class ColumnType
{
  Integer,
  Double,
  String,
  TS_LonLat,
  TS_LatLon,
  DateTime,
  None
};
enum class ColumnSelection
{
  Requested,
  Automatic,
  AutomaticRequested,
  AutomaticOnly
};

using ColumnExpression = std::string (*)(const std::string &tableColumnName,
                                         const std::string &queryColumnName);

struct Column
{
  Column(ColumnType theType,
         const std::string &theTableColumnName,
         const std::string &theQueryColumnName = "",
         ColumnExpression theExpression = nullptr,
         ColumnExpression theCoordinateExpression = nullptr)
      : itsType(theType),
        itsName(theQueryColumnName.empty() ? theTableColumnName : theQueryColumnName),
        itsTableColumnName(theTableColumnName),
        itsExpression(theExpression),
        itsCoordinateExpression(theCoordinateExpression)
  {
  }
  Column() = delete;

  bool operator==(const std::string &theQueryColumnName) const
  {
    return itsName == theQueryColumnName;
  }
  bool operator==(const Column &theOtherColumn) const { return itsName == theOtherColumn.itsName; }
  static bool columnNumberSort(const Column &first, const Column &second)
  {
    return (first.itsNumber < second.itsNumber);
  }

  ColumnType itsType;
  std::string itsName;

  const std::string &getTableColumnName() const { return itsTableColumnName; }
  friend class EngineImpl;

 private:
  std::string itsTableColumnName;
  ColumnExpression itsExpression;
  ColumnExpression itsCoordinateExpression;
  int itsNumber = -1;
  ColumnSelection itsSelection = ColumnSelection::Requested;
};

using Columns = std::list<Column>;
using ColumnTable = Column *;

struct QueryTable
{
  std::string itsName;
  std::string itsAlias;
  ColumnTable itsColumns;
  std::string itsJoin;
};

struct Table
{
  std::string itsAlias;
  Columns itsSelectedColumns;
  std::string itsJoin;

  // If set, generating only join condition (not FROM clause) for the table.
  //
  // Is only/always set for latest_messages; avidb_messages.message_id IN (SELECT message_id FROM
  // latest_messages)
  //
  bool subQuery = false;

  // If set, generating avidb_rejected_messages LEFT OUTER join for the table
  // (avidb_rejected_messages table's foreign key columns for route and message type are nullable).
  //
  // Is always set for avidb_messages_routes table; avidb_rejected_messages LEFT OUTER JOIN
  // avidb_messages_routes ON (route_id)
  // Is set for avidb_messages_types table if no message type restriction; avidb_rejected_messages
  // LEFT OUTER JOIN avidb_messages_types ON (type_id)
  //
  bool leftOuter = false;
};

using TableMap = std::map<std::string, Table>;

// Types for returning query results

using ValueVector = std::vector<TimeSeries::Value>;
using QueryValues = std::map<std::string, ValueVector>;

struct QueryData
{
  // The data has no station id; return the common value map having colum name as the map key
  //
  QueryValues &getValues(const pqxx::result::const_iterator & /* dummy */,
                         const pqxx::result::const_iterator & /* dummy */,
                         bool &duplicate)
  {
    duplicate = false;
    return itsValues;
  }

  Columns itsColumns;
  QueryValues itsValues;
  bool itsCheckDuplicateMessages =
      false;  // Always false; no check for duplicates for rejected messages
};

using StationQueryValues = std::map<StationIdType, QueryValues>;

struct StationQueryData
{
  StationQueryData(bool checkDuplicateMessages = true)
      : itsCheckDuplicateMessages(checkDuplicateMessages)
  {
  }

  QueryValues &getValues(const pqxx::result::const_iterator &row,
                         const pqxx::result::const_iterator &prevRow,
                         bool &duplicate)
  {
    // Station id is the outer level map key; return the map value (inner map having column name as
    // the map key) for the station.
    // Maintain list of station id's in the order of appearance
    //
    long stationId = row[stationIdQueryColumn].as<long>();

    std::pair<StationQueryValues::iterator, bool> stationQueryValues =
        itsValues.insert(std::make_pair(stationId, QueryValues()));
    duplicate = !stationQueryValues.second;

    if (!duplicate)
      itsStationIds.push_back(stationId);
    else if (itsCheckDuplicateMessages)
    {
      // Check for duplicate messages for the station; skip others but the 1'st
      //
      duplicate = ((row != prevRow) && (row[messageQueryColumn].as<std::string>() ==
                                        prevRow[messageQueryColumn].as<std::string>()));
    }

    return stationQueryValues.first->second;
  }

  Columns itsColumns;
  StationQueryValues itsValues;
  StationIdList itsStationIds;     // Station id's in the order returned by database query
  bool itsCheckDuplicateMessages;  // If set when querying messages, only the 1'st copy of duplicate
                                   // messages (for one of the (2; AFTN and TAFEDITOR) routes) is
                                   // returned
};

using FIRAreaAndBBox = std::pair<std::string, BBox>;
using FIRQueryData = std::map<int, FIRAreaAndBBox>;

/**
 * @brief Base class for AVI engine
 *
 * Actual implementation and private API is intentionally hidden in derived class EngineImpl.
 * All methods in this base class are defined inline in header file to avoid unresolved
 * symbols during linking in case when actual AVI engine is not loaded.
 *
 */
class Engine : public SmartMet::Spine::SmartMetEngine
{
 public:
  ~Engine() override;
  Engine() = default;

  virtual StationQueryData queryStations(QueryOptions & /* queryOptions */) const
  {
    unavailable(BCP);
  }

  virtual StationQueryData queryMessages(const StationIdList & /* stationIdList */,
                                         const QueryOptions & /* queryOptions */) const
  {
    unavailable(BCP);
  }
  virtual StationQueryData &joinStationAndMessageData(const StationQueryData & /* stationData */,
                                                      StationQueryData & /*messageData*/) const
  {
    unavailable(BCP);
  }

  virtual StationQueryData queryStationsAndMessages(QueryOptions & /*queryOptions*/) const
  {
    unavailable(BCP);
  }

  virtual QueryData queryRejectedMessages(const QueryOptions & /*queryOptions*/) const
  {
    unavailable(BCP);
  }

  virtual const FIRQueryData &queryFIRAreas() const { unavailable(BCP); }

 protected:
  void init() override {}

  void shutdown() override {}

 private:
  [[noreturn]] void unavailable(const char *file, int line, const char *function) const
  {
    throw Fmi::Exception(file, line, function, "AVI engine not available");
  }
};

// The engine

class EngineImpl;

// ======================================================================
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
