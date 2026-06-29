#define BOOST_TEST_MODULE "StationQueryDataClassModule"

#include "Config.h"
#include "EngineImpl.h"
#include "PqxxResult.h"

#include <boost/test/included/unit_test.hpp>
#include <typeinfo>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{

Fmi::Database::PostgreSQLConnectionOptions mk_connection_options(Config& itsConfig)
{
  Fmi::Database::PostgreSQLConnectionOptions opt;
  opt.host = itsConfig.getHost();
  opt.port = itsConfig.getPort();
  opt.username = itsConfig.getUsername();
  opt.password = itsConfig.getPassword();
  opt.database = itsConfig.getDatabase();
  opt.encoding = itsConfig.getEncoding();
  return opt;
}

BOOST_AUTO_TEST_CASE(stationquerydata_constructor_default)
{
  StationQueryData stationQueryData;
  BOOST_CHECK_EQUAL(stationQueryData.itsCheckDuplicateMessages, true);
}
BOOST_AUTO_TEST_CASE(stationquerydata_constructor_default_not_checkDuplicateMessages)
{
  StationQueryData stationQueryData(false);
  BOOST_CHECK_EQUAL(stationQueryData.itsCheckDuplicateMessages, false);
}
BOOST_AUTO_TEST_CASE(stationquerydata_itsColumns,
                     *boost::unit_test::depends_on("stationquerydata_constructor_default"))
{
  std::list<Column> columnsVariable;
  StationQueryData stationQueryData;
  BOOST_CHECK(typeid(stationQueryData.itsColumns) == typeid(columnsVariable));
  BOOST_CHECK_EQUAL(stationQueryData.itsColumns.size(), 0);
}

BOOST_AUTO_TEST_CASE(stationquerydata_itsValues,
                     *boost::unit_test::depends_on("stationquerydata_constructor_default"))
{
  std::map<StationIdType, QueryValues> stationQueryValuesVariable;
  StationQueryData stationQueryData;
  BOOST_CHECK(typeid(stationQueryData.itsValues) == typeid(stationQueryValuesVariable));
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 0);
}
BOOST_AUTO_TEST_CASE(stationquerydata_itsStationIds,
                     *boost::unit_test::depends_on("stationquerydata_constructor_default"))
{
  std::list<StationIdType> stationIdListVariable;
  StationQueryData stationQueryData;
  BOOST_CHECK(typeid(stationQueryData.itsStationIds) == typeid(stationIdListVariable));
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 0);
}
BOOST_AUTO_TEST_CASE(stationquerydata_itsCheckDuplicateMessages,
                     *boost::unit_test::depends_on("stationquerydata_constructor_default"))
{
  bool checkDuplicateMessagesVariable = true;
  StationQueryData stationQueryData;
  BOOST_CHECK(typeid(stationQueryData.itsCheckDuplicateMessages) ==
              typeid(checkDuplicateMessagesVariable));
  BOOST_CHECK_EQUAL(stationQueryData.itsCheckDuplicateMessages, checkDuplicateMessagesVariable);
}
BOOST_AUTO_TEST_CASE(stationquerydata_getValues_fail,
                     *boost::unit_test::depends_on("stationquerydata_constructor_default"))
{
  bool duplicate = false;
  PqxxResult emptyResult{pqxx::result()};
  StationQueryData stationQueryData;
  BOOST_CHECK_THROW(
      { QueryValues queryValues = stationQueryData.getValues(emptyResult, 0, 0, duplicate); },
      std::exception);
}
BOOST_AUTO_TEST_CASE(stationquerydata_getValues,
                     *boost::unit_test::depends_on("stationquerydata_constructor_default"))
{
  const std::string filename = "cnf/valid.conf";
  Config conf(filename);
  Fmi::Database::PostgreSQLConnection connection(mk_connection_options(conf));

  const std::string sqlStatement(
      "SELECT generate_series%2 as stationid, case when generate_series=1 then 'taf' when "
      "generate_series=2 then 'metar' else 'taf' end as "
      "message FROM generate_series(0,3) order by 1;");

  PqxxResult result{connection.executeNonTransaction(sqlStatement)};

  // result object contains the following data
  //
  //  | stationid | message |
  //  |---------------------|
  //  | 0         | taf     |  row 0
  //  | 0         | metar   |  row 1
  //  | 1         | taf     |  row 2
  //  | 1         | taf     |  row 3

  // By default, check duplicate messages
  StationQueryData stationQueryData;

  // The first row is not a dublicate in the object (new stationid 0)
  bool duplicate = true;
  QueryValues& queryValues = stationQueryData.getValues(result, 0, 0, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(duplicate, false);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The first row is not a dublicate in the object because the check of duplicate messages is ON by
  // default.
  queryValues = stationQueryData.getValues(result, 0, 0, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(duplicate, false);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The first row is not a dublicate of the second row because the check of duplicate messages is
  // ON by default.
  queryValues = stationQueryData.getValues(result, 0, 1, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(duplicate, false);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The third row is not a dublicate (new stationid 1)
  queryValues = stationQueryData.getValues(result, 2, 2, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(duplicate, false);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The third row is a dublicate for the fourth (same stationid and same message)
  queryValues = stationQueryData.getValues(result, 2, 3, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(duplicate, true);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // Do not check duplicate messages
  StationQueryData stationQueryData2(false);

  // The first row is not a dublicate (new stationid 0)
  queryValues = stationQueryData2.getValues(result, 0, 0, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(duplicate, false);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The first row is a dublicate in the object because the same stationid 0.
  queryValues = stationQueryData2.getValues(result, 0, 0, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(duplicate, true);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The first row is a dublicate of the second row because same stationid 0 and the check of
  // duplicate messages has turned OFF.
  queryValues = stationQueryData2.getValues(result, 0, 1, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(duplicate, true);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The third row is not a dublicate (new stationid 1)
  queryValues = stationQueryData2.getValues(result, 2, 2, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(duplicate, false);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The third row is a dublicate for the fourth (same stationid)
  queryValues = stationQueryData2.getValues(result, 2, 3, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(duplicate, true);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
