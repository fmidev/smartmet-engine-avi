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

const std::list<std::string> allValidParameters({"stationid",
                                                 "icao",
                                                 "name",
                                                 "latitude",
                                                 "longitude",
                                                 "lonlat",
                                                 "latlon",
                                                 "distance",
                                                 "bearing",
                                                 "elevation",
                                                 "stationvalidfrom",
                                                 "stationvalidto",
                                                 "stationmodified",
                                                 "iso2",
                                                 "messagetype",
                                                 "messagetypedescription",
                                                 "messagetypemodified",
                                                 "route",
                                                 "routedescription",
                                                 "routemodified",
                                                 "messageid",
                                                 "message",
                                                 "messagetime",
                                                 "messagevalidfrom",
                                                 "messagevalidto",
                                                 "messagecreated",
                                                 "messagefilemodified",
                                                 "messirheading",
                                                 "messageversion"});

const std::list<std::string> allValidRejectedMessagesParameters({"message",
                                                                 "messagetime",
                                                                 "messagevalidfrom",
                                                                 "messagevalidto",
                                                                 "messagecreated",
                                                                 "messagefilemodified",
                                                                 "messirheading",
                                                                 "messageversion",
                                                                 "messagerejectreason",
                                                                 "messagerejectedicao"});

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

//
// Tests for Engine::queryStations method with parameter list query option
//

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

BOOST_AUTO_TEST_CASE(engine_queryStations_with_parameterlist_queryoption_all_valid_parameters,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters = allValidParameters;
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(queryOptions.itsParameters.size(), 29);
  BOOST_CHECK_EQUAL(stationQueryData.itsColumns.size(), 3);
}

//
// Tests for Engine::queryStations method with location options
//

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_stationid,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsStationIds.push_back(10);  //!< EFJY
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsColumns.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 10);

  queryOptions.itsLocationOptions.itsStationIds.push_back(16);  //!< EFKU
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsColumns.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 10);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.back(), 16);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_stationid_fail,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsStationIds.push_back(-1);
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Spine::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_stationid_outofrange_fail,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsStationIds.push_back(std::numeric_limits<unsigned int>::max());
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Spine::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_stationid_duplicate,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsStationIds.push_back(10);
  queryOptions.itsLocationOptions.itsStationIds.push_back(16);
  queryOptions.itsLocationOptions.itsStationIds.push_back(16);
  queryOptions.itsLocationOptions.itsStationIds.push_back(10);
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_icao,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsIcaos.push_back("EFJY");
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 10);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_invalid_icao_fail,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsIcaos.push_back("XXxx");
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Spine::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_icao_duplicate,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsIcaos.push_back("EFJY");
  queryOptions.itsLocationOptions.itsIcaos.push_back("EFKU");
  queryOptions.itsLocationOptions.itsIcaos.push_back("EFKU");
  queryOptions.itsLocationOptions.itsIcaos.push_back("EFJY");
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 10);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.back(), 16);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_bbox,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsBBoxes.push_back(
      BBox(24.90696, 24.90697, 60.31582, 60.31583));  //!< EFHK
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_bbox_value_order,
    *boost::unit_test::depends_on("engine_queryStations_with_locationoption_queryoption_bbox"))
{
  BOOST_CHECK(engine != nullptr);
  // Latitude and longitude values can be given either min,max or max,min order.
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsBBoxes.push_back(BBox(29.6116, 29.6115, 62.6599, 62.6598));
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 9);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_multiple_bbox,
    *boost::unit_test::depends_on("engine_queryStations_with_locationoption_queryoption_bbox"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsBBoxes.push_back(
      BBox(29.61158, 29.61159, 62.65986, 62.65987));  //!< EFJO id=9
  queryOptions.itsLocationOptions.itsBBoxes.push_back(
      BBox(24.80458, 24.90697, 60.31582, 61.85540));  //!< EFHK id=7 and EFHA id=5
  queryOptions.itsLocationOptions.itsBBoxes.push_back(
      BBox(27.41896, 27.41897, 68.61335, 68.61336));  //!< EFIV id=8

  // The station order in the station id list is same as the order in the itsBBoxes list.
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 4);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 9);  //!< EFJO
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.back(), 8);   //!< EFIV
}

BOOST_AUTO_TEST_CASE(engine_queryStations_with_locationoption_queryoption_multiple_bbox_overlap,
                     *boost::unit_test::depends_on(
                         "engine_queryStations_with_locationoption_queryoption_multiple_bbox"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsBBoxes.push_back(
      BBox(24.80458, 25.67534, 61.85539, 62.40234));  //!< EFHA id=5 and EFJY id=10 and ILJY id=32
  queryOptions.itsLocationOptions.itsBBoxes.push_back(
      BBox(24.80458, 24.90697, 60.31582, 61.85540));  //!< EFHK id=7 and EFHA id=5

  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  auto it = stationQueryData.itsStationIds.begin();
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 4);
  BOOST_CHECK_EQUAL(*it, 10);  //!< EFJY
  ++it;
  BOOST_CHECK_EQUAL(*it, 5);  //!< EFHA
  ++it;
  BOOST_CHECK_EQUAL(*it, 7);  //!< EFHK
  ++it;
  BOOST_CHECK_EQUAL(*it, 32);  //!< ILJY
}

BOOST_AUTO_TEST_CASE(engine_queryStations_with_locationoption_queryoption_oversize_bbox,
                     *boost::unit_test::depends_on(
                         "engine_queryStations_with_locationoption_queryoption_multiple_bbox"))
{
  BOOST_CHECK(engine != nullptr);
  // Test bboxes over the edges of the WGS84 bbox [-90,-180,90,180]
  // [lat_min,lon_min,lat_max,lon_max]
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsBBoxes.push_back(BBox(-181, 181, 68.61335, 68.61336));
  queryOptions.itsLocationOptions.itsBBoxes.push_back(BBox(27.41896, 27.41897, -91, 91));
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 8);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_place,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");

  queryOptions.itsLocationOptions.itsPlaces.push_back("Inari Ivalo lentoasema");
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 8);  //!< EFIV

  queryOptions.itsLocationOptions.itsPlaces.clear();
  queryOptions.itsLocationOptions.itsPlaces.push_back("Inari Ivalo");
  queryOptions.itsLocationOptions.itsPlaces.push_back("Ivalo");
  queryOptions.itsLocationOptions.itsPlaces.push_back("*Ivalo*");
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 0);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_invalid_place_sql_injection_fail,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");

  queryOptions.itsLocationOptions.itsPlaces.push_back("'");
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Spine::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_invalid_place_sql_injection_success,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsPlaces.push_back(
      "Kajaani lentoasema')), UPPER(quote_literal('Pori lentoasema");
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_country,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsCountries = {"FI"};
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 109);

  queryOptions.itsLocationOptions.itsCountries = {"SE"};
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 162);

  queryOptions.itsLocationOptions.itsCountries = {"FI", "SE"};
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 271);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_empty_country_fail,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsCountries = {""};
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Spine::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_lonlat,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");

  // MaxDistance is not set. The result is empty.
  queryOptions.itsLocationOptions.itsLonLats.push_back(LonLat(24.90696, 60.31600));
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 0);

  // MaxDistance is too small
  queryOptions.itsLocationOptions.itsMaxDistance = 10.0;  //!< Distance is in meters
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 0);

  // Nearest station is neares than the MaxDistance.
  queryOptions.itsLocationOptions.itsMaxDistance = 1000.0;
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK

  // Inside the 14 km radius has more than two stations. Get only two of them.
  queryOptions.itsLocationOptions.itsMaxDistance = 14000.0;
  queryOptions.itsLocationOptions.itsNumberOfNearestStations = 2;
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.back(), 30);  //!< ILHK
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_wkt_polygon,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");

  // Counterclockwise polygon around EFHK station.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON((24.90695 60.31581, 24.90697 60.31581, 24.90697 60.31583, 24.90695 60.31583, "
      "24.90695 60.31581))");
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK

  // Counterclockwise polygon around EFHK and ILHK stations.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON((24.90695 60.31581, 24.95676 60.31581, 24.95676 60.3268, 24.90695 60.3268, "
      "24.90695 60.31581))");
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.back(), 30);  //!< ILHK

  // Two counterclockwise polygons. See the input and output order.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON((24.95674 60.3266, 24.95676 60.3266, 24.95676 60.3268, 24.95674 60.3268, "
      "24.95674 60.3266))");  //!< ILHK
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON((24.90695 60.31581, 24.90697 60.31581, 24.90697 60.31583, 24.90695 60.31583, "
      "24.90695 60.31581))");  //!< EFHK
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.back(), 30);  //!< ILHK

  // Clockwise polygon around EFHK station.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON((24.90695 60.31581, 24.90695 60.31583, 24.90697 60.31583, 24.90697 60.31581, "
      "24.90695 60.31581))");
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK

  // Clockwise polygon around EFHK and ILHK stations.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON((24.90695 60.31581, 24.90695 60.3268, 24.95676 60.3268, 24.95676 60.31581, "
      "24.90695 60.31581))");
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.back(), 30);  //!< ILHK

  // Two clockwise polygons. See the input and output order.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON((24.95674 60.3266, 24.95674 60.3268, 24.95676 60.3268, 24.95676 60.3266, "
      "24.95674 60.3266))");  //!< ILHK
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON((24.90695 60.31581, 24.90695 60.31583, 24.90697 60.31583, 24.90697 60.31581, "
      "24.90695 60.31581))");  //!< EFHK
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.back(), 30);  //!< ILHK

  // Select station with a counterclockwise polygon and select the same also with
  // a clocwise polygon which is inside the first polygon.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON((24.90694 60.31580, 24.90698 60.31580, 24.90698 60.31584, 24.90694 60.31584, "
      "24.90694 60.31580))");  //!< counterclockwise
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON((24.90695 60.31581, 24.90695 60.31583, 24.90697 60.31583, 24.90697 60.31581, "
      "24.90695 60.31581))");  //!< clockwise
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);

  // Two polygons intersect with each others.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON((24.90694 60.31580, 24.90698 60.31580, 24.90698 60.31584, 24.90694 60.31584, "
      "24.90694 60.31580))");  //!< counterclockwise
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON((24.90693 60.31579, 24.90695 60.31583, 24.90697 60.31583, 24.90697 60.31581, "
      "24.90693 60.31579))");  //!< clockwise polygon intersects with the counterclockwise polygon
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_wkt_polygon_with_two_loops_fail,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");

  // One polygon, with two loops: the first around EFHK and the second around ILHK station.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON((24.90695 60.31581, 24.90697 60.31581, 24.90697 60.31583, 24.90695 60.31583, "
      "24.90695 60.31581),(24.95674 60.3266, 24.95674 60.3268, 24.95676 60.3268, 24.95676 60.3266, "
      "24.95674 60.3266))");
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Spine::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_wkt_multipolygon_fail,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  // The first polygon around EFHK and the second around ILHK station.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "MULTIPOLYGON(((24.90695 60.31581, 24.90697 60.31581, 24.90697 60.31583, 24.90695 60.31583, "
      "24.90695 60.31581)),((24.95674 60.3266, 24.95674 60.3268, 24.95676 60.3268, 24.95676 "
      "60.3266, 24.95674 60.3266)))");
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Spine::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_wkt_point,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");

  // MaxDistance is not set. The result is empty.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("POINT(24.90695 60.3157)");
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 0);

  // A station is farther than MaxDistance
  queryOptions.itsLocationOptions.itsMaxDistance = 10.0;
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 0);

  // A station is nearer than MaxDistance
  queryOptions.itsLocationOptions.itsMaxDistance = 1000.0;
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_wkt_multipoint,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "MULTIPOINT((24.90695 60.3157), (24.95674 60.3266))");
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Spine::Exception);
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
