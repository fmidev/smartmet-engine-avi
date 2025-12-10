#define BOOST_TEST_MODULE "ColumnClassModule"

#include "Engine.h"

#include <boost/test/included/unit_test.hpp>
#include <typeinfo>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
const std::string stringVariable;
const bool boolVariable = true;

const ColumnType columnTypeNone = ColumnType::None;
const ColumnType columnTypeInteger = ColumnType::Integer;
const std::string tableColumnNameEmpty;
const std::string tableColumnNameNotEmpty = "columnname";

BOOST_AUTO_TEST_CASE(column_constructor)
{
  const Column column(columnTypeNone, tableColumnNameEmpty);
}

BOOST_AUTO_TEST_CASE(column_public_operator_equalto_string,
                     *boost::unit_test::depends_on("column_constructor"))
{
  const Column column(columnTypeNone, tableColumnNameEmpty);

  BOOST_CHECK(column == std::string(""));
}

BOOST_AUTO_TEST_CASE(column_public_operator_equalto_column,
                     *boost::unit_test::depends_on("column_constructor"))
{
  const Column column(columnTypeNone, tableColumnNameEmpty);
  const Column otherColumn(columnTypeInteger, tableColumnNameEmpty);
  const Column otherColumn2(columnTypeInteger, tableColumnNameNotEmpty);

  BOOST_CHECK(column == column);
  BOOST_CHECK(column == otherColumn);
  BOOST_CHECK(not(column == otherColumn2));
}

BOOST_AUTO_TEST_CASE(column_public_member_method_columnNumberSort,
                     *boost::unit_test::depends_on("column_constructor"))
{
  const Column column(columnTypeNone, tableColumnNameEmpty);
  const Column otherColumn(columnTypeInteger, tableColumnNameEmpty);
  const Column otherColumn2(columnTypeInteger, tableColumnNameNotEmpty);

  BOOST_CHECK(typeid(column.columnNumberSort(column, column)) == typeid(boolVariable));
  BOOST_CHECK(not column.columnNumberSort(column, column));
  BOOST_CHECK(not column.columnNumberSort(column, otherColumn));
  BOOST_CHECK(not column.columnNumberSort(column, otherColumn2));

  BOOST_CHECK(typeid(Column::columnNumberSort(column, column)) == typeid(boolVariable));
  BOOST_CHECK(not Column::columnNumberSort(column, column));
  BOOST_CHECK(not Column::columnNumberSort(column, otherColumn));
  BOOST_CHECK(not Column::columnNumberSort(column, otherColumn2));
}

BOOST_AUTO_TEST_CASE(column_public_member_variable_itsType,
                     *boost::unit_test::depends_on("column_constructor"))
{
  const Column column(columnTypeNone, tableColumnNameEmpty);
  const Column otherColumn(columnTypeInteger, tableColumnNameEmpty);
  Column nonConstColumn(columnTypeNone, tableColumnNameEmpty);

  BOOST_CHECK_EQUAL(column.itsType, columnTypeNone);
  BOOST_CHECK(column.itsType != columnTypeInteger);
  BOOST_CHECK_EQUAL(otherColumn.itsType, columnTypeInteger);

  // Change the value
  BOOST_CHECK(nonConstColumn.itsType == columnTypeNone);
  nonConstColumn.itsType = columnTypeInteger;
  BOOST_CHECK(nonConstColumn.itsType != columnTypeNone);
  BOOST_CHECK(nonConstColumn.itsType == columnTypeInteger);
}

BOOST_AUTO_TEST_CASE(column_public_member_variable_itsName,
                     *boost::unit_test::depends_on("column_constructor"))
{
  const Column column(columnTypeNone, tableColumnNameEmpty);
  const Column otherColumn2(columnTypeInteger, tableColumnNameNotEmpty);
  Column nonConstColumn(columnTypeNone, tableColumnNameEmpty);

  BOOST_CHECK_EQUAL(column.itsName, tableColumnNameEmpty);
  BOOST_CHECK(column.itsName != tableColumnNameNotEmpty);
  BOOST_CHECK_EQUAL(otherColumn2.itsName, tableColumnNameNotEmpty);
  BOOST_CHECK(otherColumn2.itsName != tableColumnNameEmpty);

  // Change the value
  BOOST_CHECK(nonConstColumn.itsName == tableColumnNameEmpty);
  nonConstColumn.itsName = tableColumnNameNotEmpty;
  BOOST_CHECK(nonConstColumn.itsName != tableColumnNameEmpty);
  BOOST_CHECK(nonConstColumn.itsName == tableColumnNameNotEmpty);
}

BOOST_AUTO_TEST_CASE(column_member_method_getTableColumnName,
                     *boost::unit_test::depends_on("column_constructor"))
{
  const Column column(columnTypeNone, tableColumnNameEmpty);

  BOOST_CHECK(typeid(column.getTableColumnName()) == typeid(stringVariable));
  BOOST_CHECK_EQUAL(column.getTableColumnName(), tableColumnNameEmpty);
  BOOST_CHECK(column.getTableColumnName() != tableColumnNameNotEmpty);
}

// The class has fried class Engine.
// The class does not use the following member variables and has not access to them:
// ColumnExpression itsExpression;
// ColumnExpression itsCoordinateExpression;
// ColumnSelection itsSelection;

// So we need to test the ColumnExpression by using a dummy function
std::string catenateExpression(const std::string& value1, const std::string& value2)
{
  return value1 + "," + value2;
}
BOOST_AUTO_TEST_CASE(column_columnexpression)
{
  const std::string name1 = "value1";
  const std::string name2 = "value2";
  ColumnExpression columnExpression = &catenateExpression;

  BOOST_CHECK_EQUAL((*columnExpression)(name1, name2), "value1,value2");
}
BOOST_AUTO_TEST_CASE(column_constructor_with_expression,
                     *boost::unit_test::depends_on("column_constructor"))
{
  const Column column1(
      columnTypeNone, tableColumnNameEmpty, tableColumnNameEmpty, &catenateExpression, NULL);
  const Column column2(
      columnTypeNone, tableColumnNameEmpty, tableColumnNameEmpty, NULL, &catenateExpression);
  const Column column3(columnTypeNone,
                       tableColumnNameEmpty,
                       tableColumnNameEmpty,
                       &catenateExpression,
                       &catenateExpression);
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
