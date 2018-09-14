#include "Column.h"

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
Column::Column(ColumnType theType,
               const std::string &theTableColumnName,
               const std::string &theQueryColumnName,
               ColumnExpression theExpression,
               ColumnExpression theCoordinateExpression)
    : itsType(theType),
      itsName(theQueryColumnName.empty() ? theTableColumnName : theQueryColumnName),
      itsTableColumnName(theTableColumnName),
      itsExpression(theExpression),
      itsCoordinateExpression(theCoordinateExpression),
      itsNumber(-1),
      itsSelection(Requested)
{
}

bool Column::operator==(const std::string &theQueryColumnName) const
{
  return itsName == theQueryColumnName;
}
bool Column::operator==(const Column &theOtherColumn) const
{
  return itsName == theOtherColumn.itsName;
}
bool Column::operator<(const Column &other) const
{
  return itsNumber < other.getNumber();
}
std::string Column::getExpression() const
{
  return itsExpression(itsTableColumnName, itsName);
}
std::string Column::getCoordinateExpression() const
{
  return itsCoordinateExpression(itsTableColumnName, itsName);
}
const Column::Number &Column::getNumber() const
{
  return itsNumber;
}
void Column::setNumber(const Number &number)
{
  itsNumber = number;
}
void Column::setSelection(const ColumnSelection &selection)
{
  itsSelection = selection;
}

const std::string &Column::getTableColumnName() const
{
  return itsTableColumnName;
}
bool Column::hasExpression() const
{
  return (itsExpression != nullptr);
}
bool Column::hasCoordinateExpression() const
{
  return (itsCoordinateExpression != nullptr);
}
const ColumnSelection &Column::getSelection() const
{
  return itsSelection;
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
