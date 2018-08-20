#pragma once

#include <list>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
// Types for passing location options

struct BBox
{
  BBox(double theWest, double theEast, double theSouth, double theNorth)
      : itsWest(theWest), itsEast(theEast), itsSouth(theSouth), itsNorth(theNorth)
  {
  }

  double itsWest;
  double itsEast;
  double itsSouth;
  double itsNorth;
};

struct LonLat
{
  LonLat(double theLon, double theLat) : itsLon(theLon), itsLat(theLat) {}
  double itsLon;
  double itsLat;
};

typedef long StationIdType;
typedef std::list<StationIdType> StationIdList;
typedef std::list<std::string> StringList;
typedef std::list<BBox> BBoxList;
typedef std::list<LonLat> LonLatList;

struct WKTs
{
  StringList itsWKTs;
  bool isRoute;
};

typedef struct
{
  StationIdList itsStationIds;
  StringList itsIcaos;
  BBoxList itsBBoxes;
  LonLatList itsLonLats;
  WKTs itsWKTs;
  StringList itsPlaces;
  StringList itsCountries;
  double itsMaxDistance;
  unsigned int itsNumberOfNearestStations;
} LocationOptions;

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
