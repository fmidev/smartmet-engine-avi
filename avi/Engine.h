// ======================================================================

#pragma once

#include "Config.h"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <macgyver/PostgreSQLConnection.h>
#include <spine/SmartMetEngine.h>
#include <timeseries/TimeSeries.h>
#include <list>
#include <map>
#include <pqxx/pqxx>

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

typedef long StationIdType;
typedef std::list<StationIdType> StationIdList;
typedef std::list<std::string> StringList;
typedef std::list<BBox> BBoxList;
typedef std::list<LonLat> LonLatList;

struct WKTs
{
  StringList itsWKTs;
  bool isRoute;
};

typedef struct
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
} LocationOptions;

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
      : itsMessageFormat("TAC"),
        itsMessageTypes(),
        itsParameters(),
        itsLocationOptions(),
        itsTimeOptions(),
        itsValidity(Accepted),
        itsMaxMessageStations(-1),
        itsMaxMessageRows(-1),
        itsDistinctMessages(true),
        itsFilterMETARs(true),
        itsExcludeSPECIs(false),
        itsDebug(false)
  {
  }

  std::string itsMessageFormat; // TAC, IWXXM
  StringList itsMessageTypes;
  StringList itsParameters;
  LocationOptions itsLocationOptions;
  TimeOptions itsTimeOptions;
  Validity itsValidity;  // Whether to select accepted or rejected messages

  bool itsMessageColumnSelected;  // Whether any avidb_messages column is requested or not

  int itsMaxMessageStations;  // if 0, unlimited; if < 0, engine rules
  int itsMaxMessageRows;      // if 0, unlimited; if < 0, engine rules

  bool itsDistinctMessages;  // Whether to skip duplicate messages or not

  bool itsFilterMETARs;   // Whether to filter (finnish) METARs (LIKE 'METAR%', if enabled by
                          // engine's configuration) or not
  bool itsExcludeSPECIs;  // Whether to exclude (finnish) SPECIs (if enabled with request parameter)

  bool itsDebug;  // Whether to write generated sql queries to stderr or not
};

// Types for building query

typedef enum
{
  Integer,
  Double,
  String,
  TS_LonLat,
  TS_LatLon,
  DateTime,
  None
} ColumnType;
typedef enum
{
  Requested,
  Automatic,
  AutomaticRequested,
  AutomaticOnly
} ColumnSelection;
typedef std::string (*ColumnExpression)(const std::string &tableColumnName,
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
        itsCoordinateExpression(theCoordinateExpression),
        itsNumber(-1),
        itsSelection(Requested)
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
  friend class Engine;

 private:
  std::string itsTableColumnName;
  ColumnExpression itsExpression;
  ColumnExpression itsCoordinateExpression;
  int itsNumber;
  ColumnSelection itsSelection;
};

typedef std::list<Column> Columns;
typedef Column *ColumnTable;

struct QueryTable
{
  std::string itsName;
  std::string itsAlias;
  ColumnTable itsColumns;
  std::string itsJoin;
};

struct Table
{
  Table()
  {
    subQuery = false;
    leftOuter = false;
  }

  std::string itsAlias;
  Columns itsSelectedColumns;
  std::string itsJoin;

  // If set, generating only join condition (not FROM clause) for the table.
  //
  // Is only/always set for latest_messages; avidb_messages.message_id IN (SELECT message_id FROM
  // latest_messages)
  //
  bool subQuery;

  // If set, generating avidb_rejected_messages LEFT OUTER join for the table
  // (avidb_rejected_messages table's foreign key columns for route and message type are nullable).
  //
  // Is always set for avidb_messages_routes table; avidb_rejected_messages LEFT OUTER JOIN
  // avidb_messages_routes ON (route_id)
  // Is set for avidb_messages_types table if no message type restriction; avidb_rejected_messages
  // LEFT OUTER JOIN avidb_messages_types ON (type_id)
  //
  bool leftOuter;
};

typedef std::map<std::string, Table> TableMap;

// Types for returning query results

typedef std::vector<TimeSeries::Value> ValueVector;
typedef std::map<std::string, ValueVector> QueryValues;

struct QueryData
{
  QueryData() : itsCheckDuplicateMessages(false) {}
  // The data has no station id; return the common value map having colum name as the map key
  //
  inline QueryValues &getValues(const pqxx::result::const_iterator &,
                                const pqxx::result::const_iterator &,
                                bool &duplicate)
  {
    duplicate = false;
    return itsValues;
  }

  Columns itsColumns;
  QueryValues itsValues;
  bool itsCheckDuplicateMessages;  // Always false; no check for duplicates for rejected messages
};

typedef std::map<StationIdType, QueryValues> StationQueryValues;

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

// The engine

class Engine : public SmartMet::Spine::SmartMetEngine
{
 public:
  Engine(const std::string &theConfigFileName);
  Engine() = delete;

  StationQueryData queryStations(QueryOptions &queryOptions) const;
  StationQueryData queryMessages(const StationIdList &stationIdList,
                                 const QueryOptions &queryOptions) const;
  StationQueryData &joinStationAndMessageData(const StationQueryData &stationData,
                                              StationQueryData &messageData) const;

  StationQueryData queryStationsAndMessages(QueryOptions &queryOptions) const;

  QueryData queryRejectedMessages(const QueryOptions &queryOptions) const;

 protected:
  virtual void init();
  void shutdown();

 private:
  void validateParameters(const StringList &paramList,
                          Validity validity,
                          bool &messageColumnSelected) const;
  void validateStationIds(const Fmi::Database::PostgreSQLConnection &connection,
                          const StationIdList &stationIdList,
                          bool debug) const;
  void validateIcaos(const Fmi::Database::PostgreSQLConnection &connection,
                     const StringList &icaoList,
                     bool debug) const;
  void validatePlaces(const Fmi::Database::PostgreSQLConnection &connection,
                      StringList &placeNameList,
                      bool debug) const;
  void validateCountries(const Fmi::Database::PostgreSQLConnection &connection,
                         const StringList &countryList,
                         bool debug) const;
  void validateWKTs(const Fmi::Database::PostgreSQLConnection &connection,
                    LocationOptions &locationOptions,
                    bool debug) const;
  void validateMessageTypes(const Fmi::Database::PostgreSQLConnection &connection,
                            const StringList &messageTypeList,
                            bool debug) const;

  const Column *getMessageTableTimeColumn(const std::string &timeColumn) const;

  const Column *getQueryColumn(const ColumnTable tableColumns,
                               Columns &columnList,
                               const std::string &theQueryColumnName,
                               bool &duplicate,
                               int columnNumber = -1) const;

  std::string buildStationQueryCoordinateExpressions(const Columns &columns) const;
  Columns buildStationQuerySelectClause(const StringList &paramList,
                                        bool selectStationListOnly,
                                        bool autoSelectDistance,
                                        std::string &selectClause) const;
  TableMap buildMessageQuerySelectClause(QueryTable *queryTables,
                                         const StationIdList &stationIdList,
                                         const StringList &messageTypeList,
                                         const StringList &paramList,
                                         bool routeQuery,
                                         std::string &selectClause,
                                         bool &messageColumnSelected,
                                         bool &distinct) const;

  template <typename T>
  void loadQueryResult(const pqxx::result &result,
                       bool debug,
                       T &queryData,
                       bool distinctRows = true,
                       int maxRows = 0) const;
  template <typename T>
  void executeQuery(const Fmi::Database::PostgreSQLConnection &connection,
                    const std::string &query,
                    bool debug,
                    T &queryData,
                    bool distinctRows = true,
                    int maxRows = 0) const;
  template <typename T>
  void executeParamQuery(const Fmi::Database::PostgreSQLConnection &connection,
                         const std::string &query,
                         const std::string &queryArg,
                         bool debug,
                         T &queryData,
                         bool distinctRows = true,
                         int maxRows = 0) const;
  template <typename T, typename T2>
  void executeParamQuery(const Fmi::Database::PostgreSQLConnection &connection,
                         const std::string &query,
                         const T2 &queryArgs,
                         bool debug,
                         T &queryData,
                         bool distinctRows = true,
                         int maxRows = 0) const;

  void queryStationsWithIds(const Fmi::Database::PostgreSQLConnection &connection,
                            const StationIdList &stationIdList,
                            const std::string &selectClause,
                            bool debug,
                            StationQueryData &queryData) const;
  void queryStationsWithIcaos(const Fmi::Database::PostgreSQLConnection &connection,
                              const StringList &icaoList,
                              const std::string &selectClause,
                              bool debug,
                              StationQueryData &queryData) const;
  void queryStationsWithCountries(const Fmi::Database::PostgreSQLConnection &connection,
                                  const StringList &countryList,
                                  const std::string &selectClause,
                                  bool debug,
                                  StationQueryData &stationQueryData) const;
  void queryStationsWithPlaces(const Fmi::Database::PostgreSQLConnection &connection,
                               const StringList &placeList,
                               const std::string &selectClause,
                               bool debug,
                               StationQueryData &queryData) const;
  void queryStationsWithCoordinates(const Fmi::Database::PostgreSQLConnection &connection,
                                    const LocationOptions &locationOptions,
                                    const StringList &messageTypes,
                                    const std::string &selectClause,
                                    bool debug,
                                    StationQueryData &queryData) const;
  void queryStationsWithWKTs(const Fmi::Database::PostgreSQLConnection &connection,
                             const LocationOptions &locationOptions,
                             const StringList &messageTypes,
                             const std::string &selectClause,
                             bool debug,
                             StationQueryData &queryData) const;
  void queryStationsWithBBoxes(const Fmi::Database::PostgreSQLConnection &connection,
                               const LocationOptions &locationOptions,
                               const std::string &selectClause,
                               bool debug,
                               StationQueryData &queryData) const;

  StationQueryData queryStations(const Fmi::Database::PostgreSQLConnection &connection,
                                 QueryOptions &queryOptions,
                                 bool validateQuery) const;
  StationQueryData queryMessages(const Fmi::Database::PostgreSQLConnection &connection,
                                 const StationIdList &stationIdList,
                                 const QueryOptions &queryOptions,
                                 bool validateQuery) const;

  std::string itsConfigFileName;
  std::shared_ptr<Config> itsConfig;

};  // class Engine

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet

// ======================================================================
