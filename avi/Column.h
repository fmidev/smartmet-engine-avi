#pragma once

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
  using Number = int;

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
  bool operator<(const Column &other) const { return itsNumber < other.getNumber(); }
  std::string getExpression() const { return itsExpression(itsTableColumnName, itsName); }
  std::string getCoordinateExpression() const
  {
    return itsCoordinateExpression(itsTableColumnName, itsName);
  }
  const Number &getNumber() const { return itsNumber; }
  void setNumber(const Number &number) { itsNumber = number; }
  void setSelection(const ColumnSelection &selection) { itsSelection = selection; }

  ColumnType itsType;
  std::string itsName;

  const std::string &getTableColumnName() const { return itsTableColumnName; }
  bool hasExpression() const { return (itsExpression != nullptr); }
  bool hasCoordinateExpression() const { return (itsCoordinateExpression != nullptr); }
  const ColumnSelection &getSelection() const { return itsSelection; }

 private:
  std::string itsTableColumnName;
  ColumnExpression itsExpression;
  ColumnExpression itsCoordinateExpression;
  Number itsNumber;
  ColumnSelection itsSelection;
};

struct ColumnNumberGreaterComparator
{
  bool operator()(const Column &a, const Column &b) { return a.getNumber() > b.getNumber(); }
};

typedef Column *ColumnTable;
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
