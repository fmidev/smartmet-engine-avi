#pragma once

#include "Column.h"
#include "LocationOptions.h"

#include <spine/TimeSeries.h>
#include <pqxx/pqxx>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
#define stationIdQueryColumn "stationid"
#define messageQueryColumn "message"

// Types for returning query results

typedef std::vector<SmartMet::Spine::TimeSeries::Value> ValueVector;
typedef std::map<std::string, ValueVector> QueryValues;

struct QueryData
{
  QueryData() : itsCheckDuplicateMessages(false) {}
  // The data has no station id; return the common value map having colum name as the map key
  //
  inline QueryValues &getValues(const pqxx::result::const_iterator &,
                                const pqxx::result::const_iterator &,
                                bool &duplicate)
  {
    duplicate = false;
    return itsValues;
  }

  Columns itsColumns;
  QueryValues itsValues;
  bool itsCheckDuplicateMessages;  // Always false; no check for duplicates for rejected messages
};

typedef std::map<StationIdType, QueryValues> StationQueryValues;

struct StationQueryData
{
  StationQueryData(bool checkDuplicateMessages = true)
      : itsCheckDuplicateMessages(checkDuplicateMessages)
  {
  }

  QueryValues &getValues(const pqxx::result::const_iterator &row,
                         const pqxx::result::const_iterator &prevRow,
                         bool &duplicate)
  {
    // Station id is the outer level map key; return the map value (inner map having column name as
    // the map key) for the station.
    // Maintain list of station id's in the order of appearance
    //
    long stationId = row[stationIdQueryColumn].as<long>();

    std::pair<StationQueryValues::iterator, bool> stationQueryValues =
        itsValues.insert(std::make_pair(stationId, QueryValues()));
    duplicate = !stationQueryValues.second;

    if (!duplicate)
      itsStationIds.push_back(stationId);
    else if (itsCheckDuplicateMessages)
    {
      // Check for duplicate messages for the station; skip others but the 1'st
      //
      duplicate = ((row != prevRow) && (row[messageQueryColumn].as<std::string>() ==
                                        prevRow[messageQueryColumn].as<std::string>()));
    }

    return stationQueryValues.first->second;
  }

  Columns itsColumns;
  StationQueryValues itsValues;
  StationIdList itsStationIds;     // Station id's in the order returned by database query
  bool itsCheckDuplicateMessages;  // If set when querying messages, only the 1'st copy of duplicate
                                   // messages (for one of the (2; AFTN and TAFEDITOR) routes) is
                                   // returned
};

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
