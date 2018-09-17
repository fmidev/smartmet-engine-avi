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
  using Name = std::string;
  using Number = int;

  Column(ColumnType theType,
         const std::string &theTableColumnName,
         const std::string &theQueryColumnName = "",
         ColumnExpression theExpression = nullptr,
         ColumnExpression theCoordinateExpression = nullptr);
  Column() = delete;

  bool operator==(const std::string &theQueryColumnName) const;
  bool operator==(const Column &theOtherColumn) const;
  bool operator<(const Column &other) const;

  std::string getCoordinateExpression() const;
  std::string getExpression() const;
  const Number &getNumber() const;
  const ColumnSelection &getSelection() const;
  const std::string &getTableColumnName() const;
  const ColumnType &getType() const;
  const Name &getName() const;

  void setNumber(const Number &number);
  void setSelection(const ColumnSelection &selection);

  bool hasExpression() const;
  bool hasCoordinateExpression() const;

 private:
  ColumnType itsType;
  Name itsName;
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
