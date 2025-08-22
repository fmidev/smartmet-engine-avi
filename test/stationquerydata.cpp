#define BOOST_TEST_MODULE "StationQueryDataClassModule"

#include "Config.h"
#include "EngineImpl.h"

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
  pqxx::result pqxxResult;
  StationQueryData stationQueryData;
  BOOST_CHECK_THROW(
      {
        QueryValues queryValues =
            stationQueryData.getValues(pqxxResult.begin(), pqxxResult.end(), duplicate);
      },
      pqxx::argument_error);
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

  pqxx::result pqxxResult = connection.executeNonTransaction(sqlStatement);

  // pqxxResult object contains the following data
  //
  //  | stationid | message |
  //  |---------------------|
  //  | 0         | taf     |
  //  | 0         | metar   |
  //  | 1         | taf     |
  //  | 1         | taf     |

  // for (auto it = pqxxResult.begin(); it != pqxxResult.end(); it++)
  //  std::cerr << "stationid=" << it["stationid"].as<long>() << " message='" <<
  //      it["message"].as<std::string>() << "'\n";

  pqxx::result::const_iterator row1 = pqxxResult.begin();
  pqxx::result::const_iterator row2 = pqxxResult.begin() + 1;
  pqxx::result::const_iterator row3 = pqxxResult.begin() + 2;
  pqxx::result::const_iterator row4 = pqxxResult.begin() + 3;

  // By default, check duplicate messages
  StationQueryData stationQueryData;

  // The first row is not a dublicate in the object (new stationid 0)
  bool duplicate = true;
  QueryValues& queryValues = stationQueryData.getValues(row1, row1, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(duplicate, false);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The first row is not a dublicate in the object because the check of duplicate messages is ON by
  // default.
  queryValues = stationQueryData.getValues(row1, row1, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(duplicate, false);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The first row is not a dublicate of the second row because the check of duplicate messages is
  // ON by default.
  queryValues = stationQueryData.getValues(row1, row2, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(duplicate, false);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The third row is not a dublicate (new stationid 1)
  queryValues = stationQueryData.getValues(row3, row3, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(duplicate, false);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The third row is a dublicate for the fourth (same stationid and same message)
  queryValues = stationQueryData.getValues(row3, row4, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(duplicate, true);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // Do not check duplicate messages
  StationQueryData stationQueryData2(false);

  // The first row is not a dublicate (new stationid 0)
  queryValues = stationQueryData2.getValues(row1, row1, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(duplicate, false);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The first row is a dublicate in the object because the same stationid 0.
  queryValues = stationQueryData2.getValues(row1, row1, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(duplicate, true);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The first row is a dublicate of the second row because same stationid 0 and the check of
  // duplicate messages has turned OFF.
  queryValues = stationQueryData2.getValues(row1, row2, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(duplicate, true);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The third row is not a dublicate (new stationid 1)
  queryValues = stationQueryData2.getValues(row3, row3, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(duplicate, false);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // The third row is a dublicate for the fourth (same stationid)
  queryValues = stationQueryData2.getValues(row3, row4, duplicate);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(duplicate, true);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
