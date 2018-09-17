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
const std::string tableColumnNameNotEmpty(std::string("columnname"));

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

BOOST_AUTO_TEST_CASE(column_public_less_operator,
                     *boost::unit_test::depends_on("column_constructor"))
{
  Column column(columnTypeNone, tableColumnNameEmpty);
  BOOST_CHECK((column < column) != boolVariable);

  // Default number is -1
  Column otherColumn(columnTypeInteger, tableColumnNameEmpty);
  otherColumn.setNumber(-1);
  BOOST_CHECK(not(column < otherColumn));

  otherColumn.setNumber(0);
  BOOST_CHECK(column < otherColumn);
}

BOOST_AUTO_TEST_CASE(column_public_member_variable_getType,
                     *boost::unit_test::depends_on("column_constructor"))
{
  const Column column(columnTypeNone, tableColumnNameEmpty);
  const Column otherColumn(columnTypeInteger, tableColumnNameEmpty);

  BOOST_CHECK_EQUAL(column.getType(), columnTypeNone);
  BOOST_CHECK(column.getType() != columnTypeInteger);
  BOOST_CHECK_EQUAL(otherColumn.getType(), columnTypeInteger);
}

BOOST_AUTO_TEST_CASE(column_public_member_variable_getName,
                     *boost::unit_test::depends_on("column_constructor"))
{
  const Column column(columnTypeNone, tableColumnNameEmpty);
  const Column otherColumn2(columnTypeInteger, tableColumnNameNotEmpty);

  BOOST_CHECK_EQUAL(column.getName(), tableColumnNameEmpty);
  BOOST_CHECK(column.getName() != tableColumnNameNotEmpty);
  BOOST_CHECK_EQUAL(otherColumn2.getName(), tableColumnNameNotEmpty);
  BOOST_CHECK(otherColumn2.getName() != tableColumnNameEmpty);
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

BOOST_AUTO_TEST_CASE(columns_constructor, *boost::unit_test::depends_on("column_constructor"))
{
  ColumnList columns;
  BOOST_CHECK_EQUAL(columns.size(), 0);
}

BOOST_AUTO_TEST_CASE(columns_sort_by_number, *boost::unit_test::depends_on("columns_constructor"))
{
  Column column1(columnTypeNone, tableColumnNameEmpty);
  column1.setNumber(1);
  Column column2(columnTypeNone, tableColumnNameEmpty);
  column2.setNumber(2);
  Column column3(columnTypeNone, tableColumnNameEmpty);
  column3.setNumber(3);

  ColumnList columns;
  columns.push_back(column2);
  columns.push_back(column1);
  columns.push_back(column3);

  BOOST_CHECK_EQUAL(columns.size(), 3);
  BOOST_CHECK_EQUAL(columns.front().getNumber(), 2);
  BOOST_CHECK_EQUAL(columns.back().getNumber(), 3);

  columns.sort();
  BOOST_CHECK_EQUAL(columns.front().getNumber(), 1);
  BOOST_CHECK_EQUAL(columns.back().getNumber(), 3);

  columns.descendingNumberOrder();
  BOOST_CHECK_EQUAL(columns.front().getNumber(), 3);
  BOOST_CHECK_EQUAL(columns.back().getNumber(), 1);

  columns.ascendingNumberOrder();
  BOOST_CHECK_EQUAL(columns.front().getNumber(), 1);
  BOOST_CHECK_EQUAL(columns.back().getNumber(), 3);

  BOOST_CHECK_EQUAL(columns.size(), 3);
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
