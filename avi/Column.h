#pragma once

#include <list>
#include <spine/Exception.h>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
// Types for building query

typedef enum
{
  Integer,
  Double,
  String,
  TS_LonLat,
  TS_LatLon,
  DateTime,
  None
} ColumnType;
typedef enum
{
  Requested,
  Automatic,
  AutomaticRequested,
  AutomaticOnly
} ColumnSelection;
typedef std::string (*ColumnExpression)(const std::string &tableColumnName,
                                        const std::string &queryColumnName);

struct Column
{
  Column(ColumnType theType,
         const std::string &theTableColumnName,
         const std::string &theQueryColumnName = "",
         ColumnExpression theExpression = nullptr,
         ColumnExpression theCoordinateExpression = nullptr)
      : itsType(theType),
        itsName(theQueryColumnName.empty() ? theTableColumnName : theQueryColumnName),
        itsTableColumnName(theTableColumnName),
        itsExpression(theExpression),
        itsCoordinateExpression(theCoordinateExpression),
        itsNumber(-1),
        itsSelection(Requested)
  {
  }
  Column() = delete;

  bool operator==(const std::string &theQueryColumnName) const
  {
    return itsName == theQueryColumnName;
  }
  bool operator==(const Column &theOtherColumn) const { return itsName == theOtherColumn.itsName; }
  static bool columnNumberSort(const Column &first, const Column &second)
  {
    return (first.itsNumber < second.itsNumber);
  }

  ColumnType itsType;
  std::string itsName;

  const std::string &getTableColumnName() const { return itsTableColumnName; }
  friend class Engine;

 private:
  std::string itsTableColumnName;
  ColumnExpression itsExpression;
  ColumnExpression itsCoordinateExpression;
  int itsNumber;
  ColumnSelection itsSelection;
};

typedef std::list<Column> Columns;
typedef Column *ColumnTable;

namespace
{
// ----------------------------------------------------------------------
/*!
 * \brief Sort columns by their position in the 'param=' list of the request
 */
// ----------------------------------------------------------------------

void sortColumnList(Columns &columns)
{
  try
  {
    columns.sort(columns.front().columnNumberSort);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Operation failed!");
  }
}
};  // namespace

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
