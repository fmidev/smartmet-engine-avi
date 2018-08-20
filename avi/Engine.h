// ======================================================================

#pragma once

#include "Column.h"
#include "Config.h"
#include "QueryData.h"
#include "QueryOptions.h"
#include "Table.h"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <spine/SmartMetEngine.h>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
// The engine

class Connection;

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
  void validateStationIds(const Connection &connection,
                          const StationIdList &stationIdList,
                          bool debug) const;
  void validateIcaos(const Connection &connection, const StringList &icaoList, bool debug) const;
  void validateCountries(const Connection &connection,
                         const StringList &countryList,
                         bool debug) const;
  void validateWKTs(const Connection &connection,
                    LocationOptions &locationOptions,
                    bool debug) const;
  void validateMessageTypes(const Connection &connection,
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
                                         const StringList &messageTypeList,
                                         const StringList &paramList,
                                         bool routeQuery,
                                         std::string &selectClause,
                                         bool &messageColumnSelected,
                                         bool &distinct) const;

  template <typename T>
  void executeQuery(const Connection &connection,
                    const std::string &query,
                    bool debug,
                    T &queryData,
                    bool distinctRows = true,
                    int maxRows = 0) const;

  void queryStationsWithIds(const Connection &connection,
                            const StationIdList &stationIdList,
                            const std::string &selectClause,
                            bool debug,
                            StationQueryData &queryData) const;
  void queryStationsWithIcaos(const Connection &connection,
                              const StringList &icaoList,
                              const std::string &selectClause,
                              bool debug,
                              StationQueryData &queryData) const;
  void queryStationsWithCountries(const Connection &connection,
                                  const StringList &countryList,
                                  const std::string &selectClause,
                                  bool debug,
                                  StationQueryData &stationQueryData) const;
  void queryStationsWithPlaces(const Connection &connection,
                               const StringList &placeList,
                               const std::string &selectClause,
                               bool debug,
                               StationQueryData &queryData) const;
  void queryStationsWithCoordinates(const Connection &connection,
                                    const LocationOptions &locationOptions,
                                    const StringList &messageTypes,
                                    const std::string &selectClause,
                                    bool debug,
                                    StationQueryData &queryData) const;
  void queryStationsWithWKTs(const Connection &connection,
                             const LocationOptions &locationOptions,
                             const std::string &selectClause,
                             bool debug,
                             StationQueryData &queryData) const;
  void queryStationsWithBBoxes(const Connection &connection,
                               const LocationOptions &locationOptions,
                               const std::string &selectClause,
                               bool debug,
                               StationQueryData &queryData) const;

  StationQueryData queryStations(const Connection &connection, QueryOptions &queryOptions) const;
  StationQueryData queryMessages(const Connection &connection,
                                 const StationIdList &stationIdList,
                                 const QueryOptions &queryOptions) const;

  std::string itsConfigFileName;
  std::shared_ptr<Config> itsConfig;

};  // class Engine

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet

// ======================================================================
