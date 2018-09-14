#define BOOST_TEST_MODULE "QueryDataClassModule"

#include "Config.h"
#include "Connection.h"
#include "Engine.h"

#include <boost/test/included/unit_test.hpp>
#include <typeinfo>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
BOOST_AUTO_TEST_CASE(querydata_constructor_default)
{
  QueryData queryData;
}

BOOST_AUTO_TEST_CASE(querydata_member_itsColumns,
                     *boost::unit_test::depends_on("querydata_constructor_default"))
{
  ColumnList columnsVariable;

  QueryData queryData;
  BOOST_CHECK(typeid(queryData.itsColumns) == typeid(columnsVariable));
  BOOST_CHECK_EQUAL(queryData.itsColumns.size(), 0);
}

BOOST_AUTO_TEST_CASE(querydata_member_itsValues,
                     *boost::unit_test::depends_on("querydata_constructor_default"))
{
  std::map<std::string, std::vector<SmartMet::Spine::TimeSeries::Value> > queryValuesVariable;

  QueryData queryData;
  BOOST_CHECK(typeid(queryData.itsValues) == typeid(queryValuesVariable));
  BOOST_CHECK_EQUAL(queryData.itsValues.size(), 0);
}

BOOST_AUTO_TEST_CASE(querydata_member_itsCheckDuplicateMessages,
                     *boost::unit_test::depends_on("querydata_constructor_default"))
{
  const bool boolVariable = true;

  QueryData queryData;
  BOOST_CHECK(typeid(queryData.itsCheckDuplicateMessages) == typeid(boolVariable));
  BOOST_CHECK_EQUAL(queryData.itsCheckDuplicateMessages, not boolVariable);
}

BOOST_AUTO_TEST_CASE(querydata_member_getValues,
                     *boost::unit_test::depends_on("querydata_member_itsValues"))
{
  const std::string filename = "cnf/valid.conf";
  Config conf(filename);
  Connection connection(conf.getHost(),
                        conf.getPort(),
                        conf.getUsername(),
                        conf.getPassword(),
                        conf.getDatabase(),
                        conf.getEncoding());

  const std::string sqlStatement(
      "SELECT generate_series%2 as stationid, case when generate_series=1 then 'taf' when "
      "generate_series=2 then 'metar' else 'taf' end as "
      "message FROM generate_series(0,3) order by 1;");

  QueryData queryData;

  // Try with an empty result
  pqxx::result pqxxResult;

  // The method does not pick anything from the pqxx result.
  // The result iterators are not even used inside the method.
  // The return value of duplicate attribute is always false.
  bool duplicate = true;
  QueryValues& queryValues = queryData.getValues(pqxxResult.begin(), pqxxResult.end(), duplicate);
  BOOST_CHECK_EQUAL(duplicate, false);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);

  // Try with some values in the result
  pqxxResult = connection.executeNonTransaction(sqlStatement);

  duplicate = true;
  queryValues = queryData.getValues(pqxxResult.begin(), pqxxResult.end(), duplicate);
  BOOST_CHECK_EQUAL(duplicate, false);
  BOOST_CHECK_EQUAL(queryValues.size(), 0);
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
