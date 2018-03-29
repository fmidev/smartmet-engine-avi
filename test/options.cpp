#define BOOST_TEST_MODULE "OptionsClassModule"

#include "Engine.h"

#include <boost/test/included/unit_test.hpp>
#include <typeinfo>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
BOOST_AUTO_TEST_CASE(locationoptions_members)
{
  const double doubleVariable = 0.0;

  const BBox bboxVariable(doubleVariable, doubleVariable, doubleVariable, doubleVariable);
  BOOST_CHECK(typeid(bboxVariable.itsWest) == typeid(doubleVariable));
  BOOST_CHECK_EQUAL(bboxVariable.itsWest, doubleVariable);
  BOOST_CHECK(typeid(bboxVariable.itsEast) == typeid(doubleVariable));
  BOOST_CHECK_EQUAL(bboxVariable.itsEast, doubleVariable);
  BOOST_CHECK(typeid(bboxVariable.itsSouth) == typeid(doubleVariable));
  BOOST_CHECK_EQUAL(bboxVariable.itsSouth, doubleVariable);
  BOOST_CHECK(typeid(bboxVariable.itsNorth) == typeid(doubleVariable));
  BOOST_CHECK_EQUAL(bboxVariable.itsNorth, doubleVariable);

  const LonLat lonLatVariable(doubleVariable, doubleVariable);
  BOOST_CHECK(typeid(lonLatVariable.itsLon) == typeid(doubleVariable));
  BOOST_CHECK_EQUAL(lonLatVariable.itsLon, doubleVariable);
  BOOST_CHECK(typeid(lonLatVariable.itsLat) == typeid(doubleVariable));
  BOOST_CHECK_EQUAL(lonLatVariable.itsLat, doubleVariable);

  const std::list<std::string> stringListVariable;
  const bool boolVariable = true;

  WKTs wktsVariable;
  BOOST_CHECK(typeid(wktsVariable.itsWKTs) == typeid(stringListVariable));
  BOOST_CHECK_EQUAL(wktsVariable.itsWKTs.size(), stringListVariable.size());
  BOOST_CHECK(typeid(wktsVariable.isRoute) == typeid(boolVariable));
  // Unable to test the value of isRoute bacause it is uninitialized
  // BOOST_CHECK_EQUAL(wktsVariable.isRoute, boolVariable);

  const unsigned int unsignedIntVariable = 0;
  const std::list<long> stationIdListVariable;
  const std::list<BBox> bboxListVariable;
  const std::list<LonLat> lonLatListVariable;

  LocationOptions locationOptions;
  BOOST_CHECK(typeid(locationOptions.itsStationIds) == typeid(stationIdListVariable));
  BOOST_CHECK_EQUAL(locationOptions.itsStationIds.size(), stringListVariable.size());

  BOOST_CHECK(typeid(locationOptions.itsIcaos) == typeid(stringListVariable));
  BOOST_CHECK_EQUAL(locationOptions.itsIcaos.size(), stringListVariable.size());

  BOOST_CHECK(typeid(locationOptions.itsBBoxes) == typeid(bboxListVariable));
  BOOST_CHECK_EQUAL(locationOptions.itsBBoxes.size(), bboxListVariable.size());

  BOOST_CHECK(typeid(locationOptions.itsLonLats) == typeid(lonLatListVariable));
  BOOST_CHECK_EQUAL(locationOptions.itsLonLats.size(), lonLatListVariable.size());

  BOOST_CHECK(typeid(locationOptions.itsWKTs) == typeid(wktsVariable));

  BOOST_CHECK(typeid(locationOptions.itsPlaces) == typeid(stringListVariable));
  BOOST_CHECK_EQUAL(locationOptions.itsPlaces.size(), stringListVariable.size());

  BOOST_CHECK(typeid(locationOptions.itsCountries) == typeid(stringListVariable));
  BOOST_CHECK_EQUAL(locationOptions.itsCountries.size(), stringListVariable.size());

  BOOST_CHECK(typeid(locationOptions.itsMaxDistance) == typeid(doubleVariable));
  // Unable to test the value of itsMaxDistance bacause it is uninitialized
  // BOOST_CHECK_EQUAL(locationOptions.itsMaxDistance, doubleVariable);

  BOOST_CHECK(typeid(locationOptions.itsNumberOfNearestStations) == typeid(unsignedIntVariable));
  // Unable to test the value of itsNumberOfNearestStations bacause it is uninitialized
  // BOOST_CHECK_EQUAL(locationOptions.itsNumberOfNearestStations, unsignedIntVariable);
}

BOOST_AUTO_TEST_CASE(timeoptions_members)
{
  const std::string stringVariable;
  const bool boolVariable = true;

  TimeOptions timeOptionsVariable;
  BOOST_CHECK(typeid(timeOptionsVariable.itsObservationTime) == typeid(stringVariable));
  BOOST_CHECK_EQUAL(timeOptionsVariable.itsObservationTime, stringVariable);

  BOOST_CHECK(typeid(timeOptionsVariable.itsStartTime) == typeid(stringVariable));
  BOOST_CHECK_EQUAL(timeOptionsVariable.itsStartTime, stringVariable);

  BOOST_CHECK(typeid(timeOptionsVariable.itsEndTime) == typeid(stringVariable));
  BOOST_CHECK_EQUAL(timeOptionsVariable.itsEndTime, stringVariable);

  BOOST_CHECK(typeid(timeOptionsVariable.itsTimeFormat) == typeid(stringVariable));
  BOOST_CHECK_EQUAL(timeOptionsVariable.itsTimeFormat, stringVariable);

  BOOST_CHECK(typeid(timeOptionsVariable.itsTimeZone) == typeid(stringVariable));
  BOOST_CHECK_EQUAL(timeOptionsVariable.itsTimeZone, stringVariable);

  BOOST_CHECK(typeid(timeOptionsVariable.itsQueryValidRangeMessages) == typeid(boolVariable));
  BOOST_CHECK_EQUAL(timeOptionsVariable.itsQueryValidRangeMessages, boolVariable);

  BOOST_CHECK(typeid(timeOptionsVariable.getMessageTableTimeRangeColumn()) ==
              typeid(stringVariable));
  BOOST_CHECK_EQUAL(timeOptionsVariable.getMessageTableTimeRangeColumn(),
                    std::string("messagetime"));
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
