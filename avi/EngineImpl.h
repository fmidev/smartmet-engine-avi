#pragma once

#include "Engine.h"
#include <macgyver/PostgreSQLConnection.h>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{

class EngineImpl : public Engine
{
 public:
  EngineImpl(const std::string &theConfigFileName);
  EngineImpl() = delete;

  StationQueryData queryStations(QueryOptions &queryOptions) const override;
  StationQueryData queryMessages(const StationIdList &stationIdList,
                                 const QueryOptions &queryOptions) const override;
  StationQueryData &joinStationAndMessageData(const StationQueryData &stationData,
                                              StationQueryData &messageData) const override;

  StationQueryData queryStationsAndMessages(QueryOptions &queryOptions) const override;

  QueryData queryRejectedMessages(const QueryOptions &queryOptions) const override;

 protected:
  virtual void init();
  void shutdown();

 private:
  void validateTimes(const TimeOptions &timeOptions) const;
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

};  // class EngineImpl

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
