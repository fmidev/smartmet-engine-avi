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

const std::list<std::string> allLocationParameters({"stationid",
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
                                                    "iso2"});

const std::list<std::string> allMessageTypesParameters({"messagetype",
                                                        "messagetypedescription",
                                                        "messagetypemodified"});

const std::list<std::string> allMessageSourceParameters({"route",
                                                         "routedescription",
                                                         "routemodified"});

const std::list<std::string> allMessageParameters({"messageid",
                                                   "message",
                                                   "messagetime",
                                                   "messagevalidfrom",
                                                   "messagevalidto",
                                                   "messagecreated",
                                                   "messagefilemodified",
                                                   "messirheading",
                                                   "messageversion"});

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
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 3);
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
    engine_queryStations_with_locationoption_queryoption_place_with_special_chars,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsPlaces.push_back(
      "Cabo 1° Juan Román Airport");  //!< SCAS id=5162
  queryOptions.itsLocationOptions.itsPlaces.push_back(
      "RIOgaleão ¿ Tom Jobim International Airport");                          //!< SBGL id=5028
  queryOptions.itsLocationOptions.itsPlaces.push_back("YUMA/88D");             //!< KYUX id=442
  queryOptions.itsLocationOptions.itsPlaces.push_back("R & R Farms Airport");  //!< 80NE id=14691
  queryOptions.itsLocationOptions.itsPlaces.push_back(
      "Ürümqi Diwopu International Airport");                          //!< ZWWW id=8059
  queryOptions.itsLocationOptions.itsPlaces.push_back("FRESNO(VOR)");  //!< KCZQ id=497
  queryOptions.itsLocationOptions.itsPlaces.push_back(
      "Bia¿ystok-Krywlany Airport");  //!< EPBK id=16592

  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 7);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 5162);
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

  // Nearest station is nearer than the MaxDistance.
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

const std::string ILHK_counterclockwise =
    "(24.95673 60.3265, 24.95677 60.3265, 24.95677 60.3269, 24.95673 60.3269, 24.95673 60.3265)";
const std::string ILHK_clockwise =
    "(24.95674 60.3266, 24.95674 60.3268, 24.95676 60.3268, 24.95676 60.3266, 24.95674 60.3266)";
const std::string ILHK_EFHK_counterclockwise =
    "(24.90695 60.31581, 24.95676 60.31581, 24.95676 60.3268, 24.90695 60.3268, 24.90695 60.31581)";
const std::string ILHK_EFHK_clockwise =
    "(24.90695 60.31581, 24.90695 60.3268, 24.95676 60.3268, 24.95676 60.31581, 24.90695 60.31581)";
const std::string EFHK_clockwise =
    "(24.90695 60.31581, 24.90695 60.31583, 24.90697 60.31583, 24.90697 60.31581, 24.90695 "
    "60.31581)";
const std::string EFHK_counterclockwise =
    "(24.90694 60.31580, 24.90698 60.31580, 24.90698 60.31584, 24.90694 60.31584, 24.90694 "
    "60.31580)";

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_wkt_polygon,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);

  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");

  // Counterclockwise polygon around EFHK station.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("POLYGON(" + EFHK_counterclockwise +
                                                            ")");
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK

  // Counterclockwise polygon around EFHK and ILHK stations.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("POLYGON(" +
                                                            ILHK_EFHK_counterclockwise + ")");
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.back(), 30);  //!< ILHK

  // Two counterclockwise polygons. See the input and output order.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("POLYGON(" + ILHK_counterclockwise +
                                                            ")");
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("POLYGON(" + EFHK_counterclockwise +
                                                            ")");
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.back(), 30);  //!< ILHK

  // A polygon with two separate counterclockwise rings.
  // This should not be representable as a single instance of Polygon.
  // See Figure 12 of OGC 06-103rc4 Simple feature access - Part 1: Common Architecture v1.2.1
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("POLYGON(" + ILHK_counterclockwise +
                                                            "," + EFHK_counterclockwise + ")");
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.back(), 30);  //!< ILHK

  // Clockwise polygon around EFHK station.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("POLYGON(" + EFHK_clockwise + ")");
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK

  // Clockwise polygon around EFHK and ILHK stations.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("POLYGON(" + ILHK_EFHK_clockwise + ")");
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.back(), 30);  //!< ILHK

  // Two clockwise polygons. See the input and output order.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("POLYGON(" + ILHK_clockwise + ")");
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("POLYGON(" + EFHK_clockwise + ")");
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 2);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 7);  //!< EFHK
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.back(), 30);  //!< ILHK

  // Select station with a counterclockwise polygon and select the same also with
  // a clocwise polygon which is inside the first polygon.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("POLYGON(" + EFHK_counterclockwise +
                                                            ")");
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("POLYGON(" + EFHK_clockwise + ")");
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);

  // Select a station with a counterclockwise ring in a polygon and select the same
  // also with a clocwise ring which is inside the first ring.
  // This should give an empty result. See Figure 11 of OGC 06-103rc4 Simple feature
  // access - Part 1: Common Architecture v1.2.1
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON(" + EFHK_counterclockwise + "," + EFHK_clockwise + ")");  //!< exterior) and interior
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);

  // Two polygons intersect with each others.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("POLYGON(" + EFHK_counterclockwise +
                                                            ")");
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON((24.90693 60.31579, 24.90695 60.31583, 24.90697 60.31583, 24.90697 60.31581, "
      "24.90693 60.31579))");  //!< clockwise polygon intersects with the counterclockwise polygon
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 1);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_wkt_polygon_non_closed_fail,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");

  // Counterclockwise non-closed polygon around EFHK station.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "POLYGON((24.90695 60.31581, 24.90697 60.31581, 24.90697 60.31583, 24.90695 60.31583))");
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Spine::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_wkt_polygon_with_two_loops_fail,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");

  // One polygon, with two loops: the first around EFHK and the second around ILHK station.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("POLYGON(" + EFHK_counterclockwise +
                                                            "," + ILHK_counterclockwise + ")");
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
      "MULTIPOLYGON((" + EFHK_counterclockwise + "),(" + ILHK_counterclockwise + "))");
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
    engine_queryStations_with_locationoption_queryoption_wkt_multipoint_fail,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "MULTIPOINT((24.90695 60.3157), (24.95674 60.3266))");
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Spine::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_wkt_linestring,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");

  // Route: Gotland --> Jyväskylä --> Joensuu
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "LINESTRING(18.5 57.5, 25.7 62.4, 29.7 62.6)");

  // MaxDistance is not set. The result is empty.
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 0);

  // A station is nearer than MaxDistance in meters from the route.
  queryOptions.itsLocationOptions.itsMaxDistance = 1500;
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 4);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 16852);  //!< ESVB id=16852
  //!< ILJY id=32
  //!< EFJY id=10
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.back(), 62);  //!< ILXD id=62

  // Route: Joensuu --> Jyväskylä --> Gotland
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "LINESTRING(29.7 62.6, 25.7 62.4, 18.5 57.5)");
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 4);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.front(), 62);  //!< ILXD id=62
  //!< EFJY id=10
  //!< ILJY id=32
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.back(), 16852);  //!< ESVB id=16852

  // Two LineString parts. This is not a route. A route has only one LineString and
  // all the other LocationOptions must be empty. The engine sets the value of
  // itsLocationOptions.itsWKTs.isRoute setting.
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.clear();
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("LINESTRING(29.7 62.6, 25.7 62.4)");
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("LINESTRING(25.7 62.4, 18.5 57.5)");
  stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 4);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_wkt_multilinestring_fail,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back(
      "MULTILINESTRING((18.5 57.5, 25.7 62.4),(25.7 62.4, 29.7 62.6))");
  BOOST_CHECK_THROW(engine->queryStations(queryOptions), Spine::Exception);
}

//
// Tests for Engine::queryStations method with parameter list query option
//

BOOST_AUTO_TEST_CASE(engine_querymessages_stationidlist_and_queryoptions_empty_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList;
  const QueryOptions queryOptions;
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Spine::Exception);
}

BOOST_AUTO_TEST_CASE(engine_querymessages_queryoptions_unknown_dummyparam_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList;
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("dummyparam");
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Spine::Exception);
}

BOOST_AUTO_TEST_CASE(engine_querymessages_queryoptions_allvalidparameters_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList;
  QueryOptions queryOptions;
  queryOptions.itsParameters = allValidParameters;
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Spine::Exception);
}

BOOST_AUTO_TEST_CASE(engine_querymessages_queryoptions_parameter_locationparameter_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList;
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allLocationParameters.front());
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Spine::Exception);
}

BOOST_AUTO_TEST_CASE(engine_querymessages_queryoptions_parameter_messagetypeparameter_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList;
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allMessageTypesParameters.front());
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Spine::Exception);
}

BOOST_AUTO_TEST_CASE(engine_querymessages_queryoptions_observationtime_iso_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList = {1};
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allMessageTypesParameters.front());

  queryOptions.itsTimeOptions.itsObservationTime = "2018-01-01";
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Spine::Exception);

  queryOptions.itsTimeOptions.itsObservationTime = "20180101T000000Z";
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Spine::Exception);

  queryOptions.itsTimeOptions.itsObservationTime = "2018-01-01T00:00:00Z";
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Spine::Exception);

  queryOptions.itsTimeOptions.itsObservationTime = "2018-01-01T00:00:00+00:00";
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Spine::Exception);
}

BOOST_AUTO_TEST_CASE(engine_querymessages_queryoptions_observationtime_timestamptz,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList = {7};  //!< EFHK
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allMessageTypesParameters.front());

  queryOptions.itsTimeOptions.itsObservationTime = "timestamptz '2015-11-17'";
  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 0);

  queryOptions.itsTimeOptions.itsObservationTime = "timestamptz '2015-11-17T00:20'";
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);

  queryOptions.itsTimeOptions.itsObservationTime = "timestamptz '2015-11-17T00:20:00'";
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);

  queryOptions.itsTimeOptions.itsObservationTime = "timestamptz '2015-11-17T00:20:00Z'";
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);

  queryOptions.itsTimeOptions.itsObservationTime = "timestamptz '2015-11-17T00:20:00+00'";
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);

  queryOptions.itsTimeOptions.itsObservationTime = "timestamptz '2015-11-17 00:20'";
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);

  queryOptions.itsTimeOptions.itsObservationTime = "timestamptz '2015-11-17 00:20:00'";
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);

  queryOptions.itsTimeOptions.itsObservationTime = "timestamptz '2015-11-17 00:20:00Z'";
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);

  queryOptions.itsTimeOptions.itsObservationTime = "timestamptz '2015-11-17 00:20:00+00'";
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);

  queryOptions.itsTimeOptions.itsObservationTime = "timestamptz '20151117'";
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 0);

  queryOptions.itsTimeOptions.itsObservationTime = "timestamptz '20151117T0020'";
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);

  queryOptions.itsTimeOptions.itsObservationTime = "timestamptz '20151117T002000'";
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);

  queryOptions.itsTimeOptions.itsObservationTime = "timestamptz '20151117T002000Z'";
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);

  queryOptions.itsTimeOptions.itsObservationTime = "timestamptz '20151117T002000+0000'";
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);
}

BOOST_AUTO_TEST_CASE(engine_querymessages_queryoptions_observationtime_current_timestamp,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList = {7};  //!< EFHK
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allMessageTypesParameters.front());

  queryOptions.itsTimeOptions.itsObservationTime = "current_timestamp";
  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 0);
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
