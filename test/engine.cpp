#define BOOST_TEST_MODULE "EngineClassModule"

#include "Engine.h"
#include "Config.h"
#include "Connection.h"

#include <boost/test/included/unit_test.hpp>
#include <spine/Options.h>
#include <spine/Reactor.h>
#include <memory>
#include <typeinfo>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
Engine *engine;

BOOST_AUTO_TEST_CASE(engine_constructor, *boost::unit_test::depends_on(""))
{
  const std::string filename = "cnf/valid.conf";
  Engine engine(filename);
}

BOOST_AUTO_TEST_CASE(engine_singleton, *boost::unit_test::depends_on("engine_constructor"))
{
  SmartMet::Spine::Options opts;
  opts.configfile = "cnf/reactor.conf";
  opts.parseConfig();

  engine = nullptr;

  SmartMet::Spine::Reactor reactor(opts);
  engine = reinterpret_cast<Engine *>(reactor.getSingleton("Avi", NULL));

  BOOST_CHECK(engine != nullptr);
}

BOOST_AUTO_TEST_CASE(engine_queryStations_with_empty_queryoptions_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Spine::Exception);
}

BOOST_AUTO_TEST_CASE(engine_queryStations_with_parameterlist_queryoption_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("dummyparam");
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Spine::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_parameterlist_queryoption_zero_length_parametername_fail,
    *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("");
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Spine::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_parameterlist_queryoption_duplicate_parametername_fail,
    *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsParameters.push_back("name");
  queryOptions.itsParameters.push_back("stationid");
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Spine::Exception);
}

BOOST_AUTO_TEST_CASE(engine_queryStations_with_valid_parameterlist_queryoption_stationid,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsColumns.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData.itsColumns.front().itsName, "stationid");
  BOOST_CHECK_EQUAL(stationQueryData.itsColumns.front().itsType, ColumnType::Integer);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 0);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 0);
}

BOOST_AUTO_TEST_CASE(engine_queryStations_with_valid_parameterlist_queryoption_name,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("name");
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsColumns.size(), 2);
  BOOST_CHECK_EQUAL(stationQueryData.itsColumns.front().itsName, "stationid");
  BOOST_CHECK_EQUAL(stationQueryData.itsColumns.front().itsType, ColumnType::Integer);
  BOOST_CHECK_EQUAL(stationQueryData.itsColumns.back().itsName, "name");
  BOOST_CHECK_EQUAL(stationQueryData.itsColumns.back().itsType, ColumnType::String);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 0);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 0);
}

BOOST_AUTO_TEST_CASE(engine_queryStations_with_parameterlist_queryoption_order,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);

  // stationid is always the first one in a result even when it is
  // not the first one in the itsParameters list of QueryOptions.
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("elevation");
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsParameters.push_back("name");
  queryOptions.itsParameters.push_back("icao");
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsColumns.size(), 4);
  auto it = stationQueryData.itsColumns.begin();
  BOOST_CHECK_EQUAL(it->itsName, "stationid");
  BOOST_CHECK_EQUAL(it->itsType, ColumnType::Integer);
  ++it;
  BOOST_CHECK_EQUAL(it->itsName, "elevation");
  BOOST_CHECK_EQUAL(it->itsType, ColumnType::Integer);
  ++it;
  BOOST_CHECK_EQUAL(it->itsName, "name");
  BOOST_CHECK_EQUAL(it->itsType, ColumnType::String);
  ++it;
  BOOST_CHECK_EQUAL(it->itsName, "icao");
  BOOST_CHECK_EQUAL(it->itsType, ColumnType::String);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 0);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 0);
}

BOOST_AUTO_TEST_CASE(engine_queryStations_with_parameterlist_queryoption_case_insensitivity_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("elevation");
  queryOptions.itsParameters.push_back("StationId");
  BOOST_CHECK_THROW(engine->queryStations(queryOptions), Spine::Exception);
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
