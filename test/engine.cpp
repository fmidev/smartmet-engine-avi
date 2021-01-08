#define BOOST_TEST_MODULE "EngineClassModule"

#include "Engine.h"
#include "Config.h"
#include "Connection.h"

#include <boost/test/included/unit_test.hpp>
#include <spine/Options.h>
#include <spine/Reactor.h>
#include <spine/TimeSeriesOutput.h>
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
                                                                 "messagerejectedreason",
                                                                 "messagerejectedicao"});

BOOST_AUTO_TEST_CASE(engine_constructor, *boost::unit_test::depends_on(""))
{
  const std::string filename = "cnf/valid.conf";
  Engine engine(filename);
}

BOOST_AUTO_TEST_CASE(engine_singleton, *boost::unit_test::depends_on("engine_constructor"))
{
  SmartMet::Spine::Options opts;
  opts.defaultlogging = false;
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
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(engine_queryStations_with_parameterlist_queryoption_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("dummyparam");
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_parameterlist_queryoption_zero_length_parametername_fail,
    *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("");
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Fmi::Exception);
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
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Fmi::Exception);
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
  BOOST_CHECK_THROW(engine->queryStations(queryOptions), Fmi::Exception);
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
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_stationid_outofrange_fail,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsStationIds.push_back(std::numeric_limits<unsigned int>::max());
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Fmi::Exception);
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
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Fmi::Exception);
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
    engine_queryStations_with_locationoption_queryoption_invalid_place_noresult,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");

  queryOptions.itsLocationOptions.itsPlaces.push_back("'");
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 0);
}

BOOST_AUTO_TEST_CASE(
    engine_queryStations_with_locationoption_queryoption_invalid_place2_noresult,
    *boost::unit_test::depends_on("engine_queryStations_with_valid_parameterlist_queryoption_name"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsPlaces.push_back(
      "Kajaani lentoasema')), UPPER(quote_literal('Pori lentoasema");
  StationQueryData stationQueryData = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsStationIds.size(), 0);
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
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Fmi::Exception);
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
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Fmi::Exception);
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
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Fmi::Exception);
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
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Fmi::Exception);
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
  BOOST_CHECK_THROW({ engine->queryStations(queryOptions); }, Fmi::Exception);
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
  BOOST_CHECK_THROW(engine->queryStations(queryOptions), Fmi::Exception);
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
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(engine_querymessages_queryoptions_unknown_dummyparam_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList;
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("dummyparam");
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(engine_querymessages_queryoptions_allvalidparameters_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList;
  QueryOptions queryOptions;
  queryOptions.itsParameters = allValidParameters;
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(engine_querymessages_queryoptions_parameter_locationparameter_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList;
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allLocationParameters.front());
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(engine_querymessages_queryoptions_parameter_messagetypeparameter_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList;
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allMessageTypesParameters.front());
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(engine_querymessages_queryoptions_observationtime_iso_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList = {1};
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allMessageTypesParameters.front());

  queryOptions.itsTimeOptions.itsObservationTime = "2018-01-01";
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);

  queryOptions.itsTimeOptions.itsObservationTime = "20180101T000000Z";
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);

  queryOptions.itsTimeOptions.itsObservationTime = "2018-01-01T00:00:00Z";
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);

  queryOptions.itsTimeOptions.itsObservationTime = "2018-01-01T00:00:00+00:00";
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);
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

BOOST_AUTO_TEST_CASE(engine_querymessages_queryoptions_starttime_endtime,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList = {7};  //!< EFHK
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allMessageTypesParameters.front());

  // Endtime is exclusive
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:19:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:20:00Z'";
  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 0);

  // Starttime and endtime are exclusive
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:20:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:20:00Z'";
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 0);

  // Starttime is inclusive
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:20:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:21:00Z'";
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_timerange_return_one_metar_message,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList = {7};  //!< EFHK
  QueryOptions queryOptions;

  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:20:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:21:00Z'";

  std::copy(
      allMessageParameters.begin(),
      allMessageParameters.end(),
      std::inserter(queryOptions.itsParameters, std::next(queryOptions.itsParameters.begin())));

  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);

  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);
  if (stationQueryData.itsValues.size() == 1)
  {
    StationQueryValues::const_iterator valuesIt = stationQueryData.itsValues.begin();
    BOOST_CHECK_EQUAL(valuesIt->first, stationIdList.front());
    BOOST_CHECK_EQUAL(valuesIt->second.size(), 9);
    if (valuesIt->second.size() == 9)
    {
      Spine::ValueFormatter vf{SmartMet::Spine::ValueFormatterParam()};
      Spine::TimeSeries::StringVisitor sv(vf, 1);

      // Test the value of each requested message parameter
      for (QueryValues::const_iterator qvIt = valuesIt->second.begin();
           qvIt != valuesIt->second.end();
           ++qvIt)
      {
        BOOST_CHECK_EQUAL(qvIt->second.size(), 1);
        auto name = qvIt->first;
        auto vvIt = qvIt->second.begin();
        if (name == "messageid")
        {
          std::string value = boost::apply_visitor(sv, *vvIt);
          BOOST_CHECK_EQUAL(value, "34867711");
        }
        else if (name == "message")
        {
          std::string value = boost::apply_visitor(sv, *vvIt);
          BOOST_CHECK_EQUAL(value, "METAR EFHK 170020Z 15012KT 9999 -RA BKN008 06/05 Q1004 NOSIG=");
        }
        else if (name == "messagetime")
        {
          std::string value = boost::apply_visitor(sv, *vvIt);
          BOOST_CHECK_EQUAL(value, "2015-11-17T00:20:00");
        }
        else if (name == "messagevalidfrom")
        {
          // For METAR messagevalidfrom is missing
          // Throws "[Out of range] Year is out of valid range: 1400..10000"
          BOOST_CHECK_THROW(boost::apply_visitor(sv, *vvIt), Fmi::Exception);
        }
        else if (name == "messagevalidto")
        {
          // For METAR messagevalidto is missing
          //  [Out of range] Year is out of valid range: 1400..10000
          BOOST_CHECK_THROW(boost::apply_visitor(sv, *vvIt), Fmi::Exception);
        }
        else if (name == "messagecreated")
        {
          std::string value = boost::apply_visitor(sv, *vvIt);
          BOOST_CHECK_EQUAL(value, "2015-11-17T00:18:02,130243");
        }
        else if (name == "messagefilemodified")
        {
          std::string value = boost::apply_visitor(sv, *vvIt);
          BOOST_CHECK_EQUAL(value, "2015-11-17T00:17:20");
        }
        else if (name == "messirheading")
        {
          std::string value = boost::apply_visitor(sv, *vvIt);
          BOOST_CHECK_EQUAL(value, "SAFI31 EFHK 170020");
        }
        else if (name == "messageversion")
        {
          std::string value = boost::apply_visitor(sv, *vvIt);
          BOOST_CHECK_EQUAL(value, "");
        }
        else
        {
          std::string msg = "Unknown response parameter name '";
          msg.append(name).append("'.");
          BOOST_FAIL(msg);
        }
      }
    }
  }
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_stations_filtered_out,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);

  const StationIdList stationIdList = {6, 27};  //!< EFHF and EFUT
  QueryOptions queryOptions;

  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:20:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:21:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsParameters.push_back(allMessageTypesParameters.front());
  queryOptions.itsMessageTypes.push_back("METAR");

  // We get two messages, even though the METAR messages from the stations are exculuded.
  // So, the engine does not do what it should.
  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 2);
  if (stationQueryData.itsValues.size() == 2)
  {
    for (const auto &id : stationIdList)
    {
      auto valuesIt = stationQueryData.itsValues.find(id);
      if (valuesIt == stationQueryData.itsValues.end())
      {
        std::string msg = "Result does not contain data for the stationId '";
        msg.append(std::to_string(stationIdList.front())).append("'.");
        BOOST_FAIL(msg);
      }

      // We requested two parameters
      BOOST_CHECK_EQUAL(valuesIt->second.size(), 2);

      Spine::ValueFormatter vf{SmartMet::Spine::ValueFormatterParam()};
      Spine::TimeSeries::StringVisitor sv(vf, 1);

      // The result contains only METARs.
      for (QueryValues::const_iterator qvIt = valuesIt->second.begin();
           qvIt != valuesIt->second.end();
           ++qvIt)
      {
        BOOST_CHECK_EQUAL(qvIt->second.size(), 1);
        auto name = qvIt->first;
        if (name == "messagetype")
        {
          for (const auto &value : qvIt->second)
          {
            std::string valueStr = boost::apply_visitor(sv, value);
            BOOST_CHECK_EQUAL(valueStr, "METAR");
          }
        }
        else if (name == "messageid")
        {
        }
        else
        {
          std::string msg = "Unknown response parameter name '";
          msg.append(name).append("'.");
          BOOST_FAIL(msg);
        }
      }
    }
  }
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_messagetype_ars,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList = {7};  //!< EFHK
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T08:42:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T08:43:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsParameters.push_back(allMessageTypesParameters.front());

  queryOptions.itsMessageTypes.push_back("ARS");
  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_messagetype_wxrep,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList = {7};  //!< EFHK
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-21T13:05:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-21T13:06:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsParameters.push_back(allMessageTypesParameters.front());

  queryOptions.itsMessageTypes.push_back("WXREP");
  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_messagetype_wrng,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList = {7};  //!< EFHK
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T08:43:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T08:44:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsParameters.push_back(allMessageTypesParameters.front());
  queryOptions.itsMessageTypes.push_back("WRNG");

  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_messagetype_taf,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList = {7};  //!< EFHK
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T02:34:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T02:35:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsParameters.push_back(allMessageTypesParameters.front());
  queryOptions.itsMessageTypes.push_back("TAF");

  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_messagetype_sigmet,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList = {7972};  //!< UMMS
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T08:20:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T08:21:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsParameters.push_back(allMessageTypesParameters.front());
  queryOptions.itsMessageTypes.push_back("SIGMET");

  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_messagetype_gafor,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList = {7};  //!< EFHK
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T02:00:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T02:01:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsParameters.push_back(allMessageTypesParameters.front());
  queryOptions.itsMessageTypes.push_back("GAFOR");

  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_messagetype_ars_and_wrng,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  const StationIdList stationIdList = {7};  //!< EFHK
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T08:42:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T08:44:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsParameters.push_back(allMessageTypesParameters.front());

  queryOptions.itsMessageTypes.push_back("ARS");
  queryOptions.itsMessageTypes.push_back("WRNG");

  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);
  if (stationQueryData.itsValues.size() == 1)
  {
    auto valuesIt = stationQueryData.itsValues.begin();
    BOOST_CHECK_EQUAL(valuesIt->second.size(), 2);
    if (valuesIt->second.size() == 2)
    {
      Spine::ValueFormatter vf{SmartMet::Spine::ValueFormatterParam()};
      Spine::TimeSeries::StringVisitor sv(vf, 1);

      for (QueryValues::const_iterator qvIt = valuesIt->second.begin();
           qvIt != valuesIt->second.end();
           ++qvIt)
      {
        BOOST_CHECK_EQUAL(qvIt->second.size(), 2);
        auto name = qvIt->first;
        if (name == "messagetype")
        {
          for (const auto &value : qvIt->second)
          {
            std::string valueStr = boost::apply_visitor(sv, value);
            if (valueStr != "ARS" and valueStr != "WRNG")
            {
              std::string msg = "Unexpected messagetype '";
              msg.append(valueStr).append("'.");
              BOOST_FAIL(msg);
            }
          }
        }
        else if (name == "messageid")
        {
        }
        else
        {
          std::string msg = "Unknown response parameter name '";
          msg.append(name).append("'.");
          BOOST_FAIL(msg);
        }
      }
    }
  }
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_stationidlist_with_multiple_stations,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  StationIdList stationIdList = {8, 9, 10, 11, -1};  //!< EFIV,EFJO,EFJY,EFKE,DUMMY
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:10:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:30:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());

  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 4);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_stationidlist_and_maxmessagestations,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  StationIdList stationIdList = {8, 9, 10, 11, -1};  //!< EFIV,EFJO,EFJY,EFKE,DUMMY
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:10:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:30:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsMaxMessageStations = 5;

  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 4);

  queryOptions.itsMaxMessageStations = 4;
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_locationsoptions_stationids_with_multiple_stations_fail,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  StationIdList stationIdList;
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:10:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:30:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsLocationOptions.itsStationIds = {8, 9, 10, 11};

  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_locationsoptions_icaos_with_multiple_stations_fail,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  StationIdList stationIdList;
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:10:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:30:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsLocationOptions.itsIcaos = {"EFIV", "EFJO", "EFJY", "EFKE"};

  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_locationsoptions_bboxes_fail,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  StationIdList stationIdList;
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:10:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:30:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsLocationOptions.itsBBoxes.push_back(
      BBox(29.61158, 29.61159, 62.65986, 62.65987));  //!< EFJO id=9
  queryOptions.itsLocationOptions.itsBBoxes.push_back(
      BBox(24.80458, 24.90697, 60.31582, 61.85540));  //!< EFHK id=7 and EFHA id=5

  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_locationsoptions_lonlats_fail,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  StationIdList stationIdList;
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:10:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:30:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsLocationOptions.itsLonLats.push_back(LonLat(24.90696, 60.31600));
  queryOptions.itsLocationOptions.itsMaxDistance = 1000.0;

  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_locationsoptions_wkts_fail,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  StationIdList stationIdList;
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:10:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:30:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsLocationOptions.itsWKTs.itsWKTs.push_back("POLYGON(" + EFHK_counterclockwise +
                                                            ")");
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_locationsoptions_places_fail,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  StationIdList stationIdList;
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:10:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:30:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsLocationOptions.itsPlaces.push_back("Inari Ivalo lentoasema");

  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_locationsoptions_countries_fail,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  StationIdList stationIdList;
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:10:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:30:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsLocationOptions.itsCountries = {"FI", "SE"};

  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_queryoptions_maxmessagerows,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  StationIdList stationIdList = {8};
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:10:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T01:10:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());

  // Negative value is ignored and the value from the configuration is used.
  queryOptions.itsMaxMessageRows = -1;
  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);
  if (stationQueryData.itsValues.size() > 0)
  {
    // One parameter requested
    StationQueryValues::const_iterator valuesIt = stationQueryData.itsValues.begin();
    BOOST_CHECK_EQUAL(valuesIt->second.size(), 1);
    if (valuesIt->second.size() > 0)
    {
      // Two messages between time interval
      QueryValues::const_iterator qvIt = valuesIt->second.begin();
      BOOST_CHECK_EQUAL(qvIt->second.size(), 2);
    }
  }

  // The zero value does not limit messages in the result
  queryOptions.itsMaxMessageRows = 0;
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);
  if (stationQueryData.itsValues.size() > 0)
  {
    // One parameter requested
    StationQueryValues::const_iterator valuesIt = stationQueryData.itsValues.begin();
    BOOST_CHECK_EQUAL(valuesIt->second.size(), 1);
    if (valuesIt->second.size() > 0)
    {
      // Two messages between time interval
      QueryValues::const_iterator qvIt = valuesIt->second.begin();
      BOOST_CHECK_EQUAL(qvIt->second.size(), 2);
    }
  }

  // Greater than 0 value limits the number of messages in result
  queryOptions.itsMaxMessageRows = 1;
  BOOST_CHECK_THROW(engine->queryMessages(stationIdList, queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_queryoptions_distinctmessages,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  StationIdList stationIdList = {7};
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:10:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T01:10:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());

  // Remove duplicates from the result. This is the default setting.
  queryOptions.itsDistinctMessages = true;
  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);
  if (stationQueryData.itsValues.size() > 0)
  {
    // One parameter requested
    StationQueryValues::const_iterator valuesIt = stationQueryData.itsValues.begin();
    BOOST_CHECK_EQUAL(valuesIt->second.size(), 1);
    if (valuesIt->second.size() > 0)
    {
      // Two messages between time interval
      QueryValues::const_iterator qvIt = valuesIt->second.begin();
      BOOST_CHECK_EQUAL(qvIt->second.size(), 2);
    }
  }

  // Request also the duplicates
  queryOptions.itsDistinctMessages = false;
  stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);
  if (stationQueryData.itsValues.size() > 0)
  {
    StationQueryValues::const_iterator valuesIt = stationQueryData.itsValues.begin();
    BOOST_CHECK_EQUAL(valuesIt->second.size(), 1);
    if (valuesIt->second.size() > 0)
    {
      // Four messages between time interval
      QueryValues::const_iterator qvIt = valuesIt->second.begin();
      BOOST_CHECK_EQUAL(qvIt->second.size(), 4);
    }
  }
}

BOOST_AUTO_TEST_CASE(
    engine_querymessages_queryoptions_filterfimetarxxx,
    *boost::unit_test::depends_on("engine_querymessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);
  StationIdList stationIdList = {8, 27};  //!< EFHK, EFUT

  // In the configuration there is filtering settings for EFHF and EFHK.
  // Here the itsFilterMETARs setting does not have any effect to the result.
  // The filtering is not supported when itsTimeOptions.itsObservationTime is used.
  // The filtering is not supported when itsTimeOptions.itsQueryValidRangeMessages is true.
  QueryOptions queryOptions;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:10:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T01:10:00Z'";
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsFilterMETARs = true;
  queryOptions.itsTimeOptions.itsQueryValidRangeMessages = false;
  queryOptions.itsMessageTypes.push_back("METAR");

  StationQueryData stationQueryData = engine->queryMessages(stationIdList, queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 2);
}

//
// Tests for Engine::queryStationsAndMessages method
//

BOOST_AUTO_TEST_CASE(engine_querystationsandmessages_queryoptions_empty_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  BOOST_CHECK_THROW(engine->queryStationsAndMessages(queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(engine_querystationsandmessages_queryoptions_with_parameter,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  StationQueryData stationQueryData = engine->queryStationsAndMessages(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 0);
}

BOOST_AUTO_TEST_CASE(
    engine_querystationsandmessages_queryoptions_with_validity_accepted,
    *boost::unit_test::depends_on("engine_querystationsandmessages_queryoptions_with_parameter"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsValidity = Avi::Accepted;
  StationQueryData stationQueryData = engine->queryStationsAndMessages(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 0);
}

BOOST_AUTO_TEST_CASE(
    engine_querystationsandmessages_queryoptions_with_validity_rejected_fail,
    *boost::unit_test::depends_on("engine_querystationsandmessages_queryoptions_with_parameter"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsValidity = Avi::Rejected;
  BOOST_CHECK_THROW(engine->queryStationsAndMessages(queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_querystationsandmessages_queryoptions_with_stationid_fail,
    *boost::unit_test::depends_on("engine_querystationsandmessages_queryoptions_with_parameter"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsLocationOptions.itsStationIds = {8};
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  BOOST_CHECK_THROW(engine->queryStationsAndMessages(queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(
    engine_querystationsandmessages_queryoptions_with_stationid_and_messageparameter,
    *boost::unit_test::depends_on("engine_querystationsandmessages_queryoptions_with_parameter"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allLocationParameters.front());
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  StationQueryData stationQueryData = engine->queryStationsAndMessages(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 0);
}

BOOST_AUTO_TEST_CASE(
    engine_querystationsandmessages_queryoptions_with_valid_values,
    *boost::unit_test::depends_on("engine_querystationsandmessages_queryoptions_with_parameter"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsLocationOptions.itsStationIds.push_back(8);
  queryOptions.itsParameters.push_back(allLocationParameters.front());
  queryOptions.itsParameters.push_back(allMessageParameters.front());
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:10:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T01:10:00Z'";
  StationQueryData stationQueryData = engine->queryStationsAndMessages(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData.itsValues.size(), 1);
}

//
// Tests for Engine::joinStationAndMessageData method
//

BOOST_AUTO_TEST_CASE(
    engine_joinstationandmessagedata_empty_inputs,
    *boost::unit_test::depends_on("engine_queryStations_with_locationoption_queryoption_stationid"))
{
  BOOST_CHECK(engine != nullptr);
  const StationQueryData stationQueryData1;
  StationQueryData stationQueryData2;
  StationQueryData joinedStationQueryData;
  joinedStationQueryData = engine->joinStationAndMessageData(stationQueryData1, stationQueryData2);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsColumns.size(), 0);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsValues.size(), 0);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsStationIds.size(), 0);
}

BOOST_AUTO_TEST_CASE(
    engine_joinstationandmessagedata_the_first_has_station,
    *boost::unit_test::depends_on("engine_queryStations_with_locationoption_queryoption_stationid"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsStationIds.push_back(10);  //!< EFJY

  const StationQueryData stationQueryData1 = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData1.itsColumns.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsStationIds.front(), 10);

  StationQueryData stationQueryData2;
  StationQueryData joinedStationQueryData;
  joinedStationQueryData = engine->joinStationAndMessageData(stationQueryData1, stationQueryData2);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsColumns.size(), 0);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsValues.size(), 0);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsStationIds.size(), 0);

  // The type of second argument is not const, so its content must be checked.
  BOOST_CHECK_EQUAL(stationQueryData2.itsColumns.size(), 0);
  BOOST_CHECK_EQUAL(stationQueryData2.itsValues.size(), 0);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 0);
}

BOOST_AUTO_TEST_CASE(
    engine_joinstationandmessagedata_the_second_has_station,
    *boost::unit_test::depends_on("engine_queryStations_with_locationoption_queryoption_stationid"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsStationIds.push_back(10);  //!< EFJY

  const StationQueryData stationQueryData1;

  StationQueryData stationQueryData2 = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData2.itsColumns.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.front(), 10);

  StationQueryData joinedStationQueryData;
  joinedStationQueryData = engine->joinStationAndMessageData(stationQueryData1, stationQueryData2);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsColumns.size(), 1);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsStationIds.front(), 10);

  // The type of second argument is not const, so its content must be checked.
  BOOST_CHECK_EQUAL(stationQueryData2.itsColumns.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.front(), 10);
}

BOOST_AUTO_TEST_CASE(
    engine_joinstationandmessagedata_the_both_have_station,
    *boost::unit_test::depends_on("engine_queryStations_with_locationoption_queryoption_stationid"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back("stationid");
  queryOptions.itsLocationOptions.itsStationIds.push_back(10);  //!< EFJY

  const StationQueryData stationQueryData1 = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData1.itsColumns.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsStationIds.front(), 10);

  queryOptions.itsLocationOptions.itsStationIds.clear();
  queryOptions.itsLocationOptions.itsStationIds.push_back(27);  //!< EFUT
  StationQueryData stationQueryData2 = engine->queryStations(queryOptions);
  BOOST_CHECK_EQUAL(stationQueryData2.itsColumns.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.front(), 27);

  // Only the station of second stationQueryData is picked to the joined container.
  StationQueryData joinedStationQueryData;
  joinedStationQueryData = engine->joinStationAndMessageData(stationQueryData1, stationQueryData2);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsColumns.size(), 1);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsStationIds.front(), 27);

  // The type of second argument is not const, so its content must be checked.
  BOOST_CHECK_EQUAL(stationQueryData2.itsColumns.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.front(), 27);
}

BOOST_AUTO_TEST_CASE(
    engine_joinstationandmessagedata_the_first_has_message,
    *boost::unit_test::depends_on("engine_querymessages_timerange_return_one_metar_message"))
{
  BOOST_CHECK(engine != nullptr);

  const StationIdList stationIdList = {7};  //!< EFHK
  QueryOptions queryOptions1;
  queryOptions1.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:20:00Z'";
  queryOptions1.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:21:00Z'";
  std::copy(
      allMessageParameters.begin(),
      allMessageParameters.end(),
      std::inserter(queryOptions1.itsParameters, std::next(queryOptions1.itsParameters.begin())));
  const StationQueryData stationQueryData1 = engine->queryMessages(stationIdList, queryOptions1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsColumns.size(), 11);
  BOOST_CHECK_EQUAL(stationQueryData1.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsStationIds.front(), 7);

  StationQueryData stationQueryData2;
  StationQueryData joinedStationQueryData;
  joinedStationQueryData = engine->joinStationAndMessageData(stationQueryData1, stationQueryData2);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsColumns.size(), 0);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsValues.size(), 0);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsStationIds.size(), 0);

  // The type of second argument is not const, so its content must be checked.
  BOOST_CHECK_EQUAL(stationQueryData2.itsColumns.size(), 0);
  BOOST_CHECK_EQUAL(stationQueryData2.itsValues.size(), 0);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 0);
}

BOOST_AUTO_TEST_CASE(
    engine_joinstationandmessagedata_the_second_has_message,
    *boost::unit_test::depends_on("engine_querymessages_timerange_return_one_metar_message"))
{
  BOOST_CHECK(engine != nullptr);

  const StationQueryData stationQueryData1;
  const StationIdList stationIdList = {7};  //!< EFHK
  QueryOptions queryOptions2;
  queryOptions2.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:20:00Z'";
  queryOptions2.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:21:00Z'";
  std::copy(
      allMessageParameters.begin(),
      allMessageParameters.end(),
      std::inserter(queryOptions2.itsParameters, std::next(queryOptions2.itsParameters.begin())));
  StationQueryData stationQueryData2 = engine->queryMessages(stationIdList, queryOptions2);
  BOOST_CHECK_EQUAL(stationQueryData2.itsColumns.size(), 11);
  BOOST_CHECK_EQUAL(stationQueryData2.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.front(), 7);

  StationQueryData joinedStationQueryData;
  joinedStationQueryData = engine->joinStationAndMessageData(stationQueryData1, stationQueryData2);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsColumns.size(), 11);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsStationIds.front(), 7);
}

BOOST_AUTO_TEST_CASE(
    engine_joinstationandmessagedata_the_first_has_station_the_second_has_message,
    *boost::unit_test::depends_on("engine_querymessages_timerange_return_one_metar_message"))
{
  BOOST_CHECK(engine != nullptr);

  QueryOptions queryOptions1;
  std::copy(
      allLocationParameters.begin(),
      allLocationParameters.end(),
      std::inserter(queryOptions1.itsParameters, std::next(queryOptions1.itsParameters.begin())));
  queryOptions1.itsLocationOptions.itsStationIds.push_back(10);  //!< EFJY

  const StationQueryData stationQueryData1 = engine->queryStations(queryOptions1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsColumns.size(), 14);
  BOOST_CHECK_EQUAL(stationQueryData1.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsStationIds.front(), 10);

  const StationIdList stationIdList = {7};  //!< EFHK
  QueryOptions queryOptions2;
  queryOptions2.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:20:00Z'";
  queryOptions2.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:21:00Z'";
  std::copy(
      allMessageParameters.begin(),
      allMessageParameters.end(),
      std::inserter(queryOptions2.itsParameters, std::next(queryOptions2.itsParameters.begin())));
  StationQueryData stationQueryData2 = engine->queryMessages(stationIdList, queryOptions2);
  BOOST_CHECK_EQUAL(stationQueryData2.itsColumns.size(), 11);
  BOOST_CHECK_EQUAL(stationQueryData2.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.front(), 7);

  StationQueryData joinedStationQueryData;
  joinedStationQueryData = engine->joinStationAndMessageData(stationQueryData1, stationQueryData2);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsColumns.size(), 11);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsStationIds.front(), 7);
}

BOOST_AUTO_TEST_CASE(
    engine_joinstationandmessagedata_the_first_has_message_the_second_has_station,
    *boost::unit_test::depends_on("engine_querymessages_timerange_return_one_metar_message"))
{
  BOOST_CHECK(engine != nullptr);

  const StationIdList stationIdList = {7};  //!< EFHK
  QueryOptions queryOptions1;
  queryOptions1.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:20:00Z'";
  queryOptions1.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:21:00Z'";
  std::copy(
      allMessageParameters.begin(),
      allMessageParameters.end(),
      std::inserter(queryOptions1.itsParameters, std::next(queryOptions1.itsParameters.begin())));

  const StationQueryData stationQueryData1 = engine->queryMessages(stationIdList, queryOptions1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsColumns.size(), 11);
  BOOST_CHECK_EQUAL(stationQueryData1.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsStationIds.front(), 7);

  QueryOptions queryOptions2;
  std::copy(
      allLocationParameters.begin(),
      allLocationParameters.end(),
      std::inserter(queryOptions2.itsParameters, std::next(queryOptions2.itsParameters.begin())));
  queryOptions2.itsLocationOptions.itsStationIds.push_back(10);  //!< EFJY

  StationQueryData stationQueryData2 = engine->queryStations(queryOptions2);
  BOOST_CHECK_EQUAL(stationQueryData2.itsColumns.size(), 14);
  BOOST_CHECK_EQUAL(stationQueryData2.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.front(), 10);

  StationQueryData joinedStationQueryData;
  joinedStationQueryData = engine->joinStationAndMessageData(stationQueryData1, stationQueryData2);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsColumns.size(), 14);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsStationIds.front(), 10);
}

BOOST_AUTO_TEST_CASE(
    engine_joinstationandmessagedata_the_first_has_station_the_second_has_message_from_the_same_station,
    *boost::unit_test::depends_on("engine_querymessages_timerange_return_one_metar_message"))
{
  BOOST_CHECK(engine != nullptr);

  QueryOptions queryOptions1;
  std::copy(
      allLocationParameters.begin(),
      allLocationParameters.end(),
      std::inserter(queryOptions1.itsParameters, std::next(queryOptions1.itsParameters.begin())));
  queryOptions1.itsLocationOptions.itsStationIds.push_back(7);  //!< EFHK

  const StationQueryData stationQueryData1 = engine->queryStations(queryOptions1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsColumns.size(), 14);
  BOOST_CHECK_EQUAL(stationQueryData1.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData1.itsStationIds.front(), 7);

  const StationIdList stationIdList = {7};  //!< EFHK
  QueryOptions queryOptions2;
  queryOptions2.itsTimeOptions.itsStartTime = "timestamptz '2015-11-17T00:20:00Z'";
  queryOptions2.itsTimeOptions.itsEndTime = "timestamptz '2015-11-17T00:21:00Z'";
  std::copy(
      allMessageParameters.begin(),
      allMessageParameters.end(),
      std::inserter(queryOptions2.itsParameters, std::next(queryOptions2.itsParameters.begin())));

  StationQueryData stationQueryData2 = engine->queryMessages(stationIdList, queryOptions2);
  BOOST_CHECK_EQUAL(stationQueryData2.itsColumns.size(), 11);
  BOOST_CHECK_EQUAL(stationQueryData2.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(stationQueryData2.itsStationIds.front(), 7);

  StationQueryData joinedStationQueryData;
  joinedStationQueryData = engine->joinStationAndMessageData(stationQueryData1, stationQueryData2);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsColumns.size(), 11);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsValues.size(), 1);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsStationIds.size(), 1);
  BOOST_CHECK_EQUAL(joinedStationQueryData.itsStationIds.front(), 7);
}

//
// Tests for QueryData queryRejectedMessages method
//

BOOST_AUTO_TEST_CASE(engine_queryrejectedmessages_queryoptions_empty_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);
  QueryOptions queryOptions;
  BOOST_CHECK_THROW(engine->queryRejectedMessages(queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(engine_queryrejectedmessages_queryoptions_two_parameters_fail,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);

  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allValidRejectedMessagesParameters.front());
  queryOptions.itsParameters.push_back(allValidRejectedMessagesParameters.back());
  BOOST_CHECK_THROW(engine->queryRejectedMessages(queryOptions), Fmi::Exception);
}

BOOST_AUTO_TEST_CASE(engine_queryrejectedmessages_queryoptions_starttime_endtime,
                     *boost::unit_test::depends_on("engine_singleton"))
{
  BOOST_CHECK(engine != nullptr);

  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allValidRejectedMessagesParameters.front());
  queryOptions.itsParameters.push_back(allValidRejectedMessagesParameters.back());
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-20T22:00:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-20T22:10:00Z'";
  QueryData queryData = engine->queryRejectedMessages(queryOptions);
  BOOST_CHECK_EQUAL(queryData.itsColumns.size(), 2);
  BOOST_CHECK_EQUAL(queryData.itsValues.size(), 2);
}

BOOST_AUTO_TEST_CASE(
    engine_queryrejectedmessages_queryoptions_produce_valid_response,
    *boost::unit_test::depends_on("engine_queryrejectedmessages_queryoptions_starttime_endtime"))
{
  BOOST_CHECK(engine != nullptr);

  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allValidRejectedMessagesParameters.front());
  queryOptions.itsParameters.push_back(allValidRejectedMessagesParameters.back());
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-20T22:00:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-20T22:10:00Z'";
  QueryData queryData = engine->queryRejectedMessages(queryOptions);

  auto f = queryData.itsValues.find(allValidRejectedMessagesParameters.front());
  auto b = queryData.itsValues.find(allValidRejectedMessagesParameters.back());

  BOOST_CHECK_EQUAL(queryData.itsColumns.size(), 2);
  BOOST_CHECK(f != queryData.itsValues.end());
  BOOST_CHECK(b != queryData.itsValues.end());
  BOOST_CHECK_EQUAL(f->second.size(), 12);
  BOOST_CHECK_EQUAL(b->second.size(), 12);
  BOOST_CHECK(queryData.itsColumns.front() == f->first);
  BOOST_CHECK(queryData.itsColumns.back() == b->first);
}

BOOST_AUTO_TEST_CASE(engine_queryrejectedmessages_queryoptions_allvalidrejectedmessagesparameters,
                     *boost::unit_test::depends_on(
                         "engine_queryrejectedmessages_queryoptions_produce_valid_response"))
{
  BOOST_CHECK(engine != nullptr);

  QueryOptions queryOptions;
  queryOptions.itsParameters = allValidRejectedMessagesParameters;
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-20T22:00:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-20T22:10:00Z'";
  QueryData queryData = engine->queryRejectedMessages(queryOptions);
  BOOST_CHECK_EQUAL(queryData.itsColumns.size(), allValidRejectedMessagesParameters.size());
}

BOOST_AUTO_TEST_CASE(engine_queryrejectedmessages_queryoptions_messagetype_metar,
                     *boost::unit_test::depends_on(
                         "engine_queryrejectedmessages_queryoptions_produce_valid_response"))
{
  BOOST_CHECK(engine != nullptr);

  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allValidRejectedMessagesParameters.front());
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-20T22:00:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-20T22:10:00Z'";
  queryOptions.itsMessageTypes.push_back("METAR");
  QueryData queryData = engine->queryRejectedMessages(queryOptions);
  auto f = queryData.itsValues.find(allValidRejectedMessagesParameters.front());
  BOOST_CHECK(f != queryData.itsValues.end());
  BOOST_CHECK_EQUAL(f->second.size(), 11);
}

BOOST_AUTO_TEST_CASE(engine_queryrejectedmessages_queryoptions_maxmessagerows,
                     *boost::unit_test::depends_on(
                         "engine_queryrejectedmessages_queryoptions_produce_valid_response"))
{
  BOOST_CHECK(engine != nullptr);

  QueryOptions queryOptions;
  queryOptions.itsParameters.push_back(allValidRejectedMessagesParameters.front());
  queryOptions.itsTimeOptions.itsStartTime = "timestamptz '2015-11-20T22:00:00Z'";
  queryOptions.itsTimeOptions.itsEndTime = "timestamptz '2015-11-20T22:10:00Z'";
  queryOptions.itsMaxMessageRows = 12;

  QueryData queryData = engine->queryRejectedMessages(queryOptions);
  auto f = queryData.itsValues.find(allValidRejectedMessagesParameters.front());
  BOOST_CHECK(f != queryData.itsValues.end());
  BOOST_CHECK_EQUAL(f->second.size(), 12);

  queryOptions.itsMaxMessageRows = 5;
  BOOST_CHECK_THROW(engine->queryRejectedMessages(queryOptions), Fmi::Exception);
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
