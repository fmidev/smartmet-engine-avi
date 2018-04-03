#define BOOST_TEST_MODULE "TableClassModule"

#include "Engine.h"

#include <boost/test/included/unit_test.hpp>
#include <typeinfo>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
BOOST_AUTO_TEST_CASE(table_constructor)
{
  Table table;
}

BOOST_AUTO_TEST_CASE(table_membervariable_type, *boost::unit_test::depends_on("table_constructor"))
{
  bool boolVariable = true;
  std::string stringVariable;
  std::list<Column> columnListVariable;

  Table table;
  BOOST_CHECK(typeid(table.itsAlias) == typeid(stringVariable));
  BOOST_CHECK(typeid(table.itsSelectedColumns) == typeid(columnListVariable));
  BOOST_CHECK(typeid(table.itsJoin) == typeid(stringVariable));
  BOOST_CHECK(typeid(table.subQuery) == typeid(boolVariable));
  BOOST_CHECK(typeid(table.leftOuter) == typeid(boolVariable));
}

BOOST_AUTO_TEST_CASE(table_membervariable_initial_value,
                     *boost::unit_test::depends_on("table_membervariable_type"))
{
  bool boolVariable = true;
  std::string stringVariable;
  std::list<Column> columnListVariable;

  Table table;
  BOOST_CHECK_EQUAL(table.itsAlias, stringVariable);
  BOOST_CHECK_EQUAL(table.itsSelectedColumns.size(), columnListVariable.size());
  BOOST_CHECK_EQUAL(table.itsJoin, stringVariable);
  BOOST_CHECK_EQUAL(table.subQuery, not boolVariable);
  BOOST_CHECK_EQUAL(table.leftOuter, not boolVariable);
}

BOOST_AUTO_TEST_CASE(tablemap_constructor, *boost::unit_test::depends_on("table_constructor"))
{
  TableMap tableMap;
}

BOOST_AUTO_TEST_CASE(tablemap_type, *boost::unit_test::depends_on("tablemap_constructor"))
{
  std::map<std::string, Table> tableMapVariable;
  TableMap tableMap;
}

BOOST_AUTO_TEST_CASE(columntable_constructor)
{
  ColumnTable columnTable = nullptr;
  BOOST_CHECK_EQUAL(columnTable, nullptr);
}

BOOST_AUTO_TEST_CASE(querytable_constructor,
                     *boost::unit_test::depends_on("columntable_constructor"))
{
  QueryTable queryTable;
}

BOOST_AUTO_TEST_CASE(querytable_membervariable_type,
                     *boost::unit_test::depends_on("querytable_constructor"))
{
  std::string stringVariable;
  Column* columnPtrVariable = nullptr;

  QueryTable queryTable;
  BOOST_CHECK(typeid(queryTable.itsName) == typeid(stringVariable));
  BOOST_CHECK(typeid(queryTable.itsAlias) == typeid(stringVariable));
  BOOST_CHECK(typeid(queryTable.itsColumns) == typeid(columnPtrVariable));
  BOOST_CHECK(typeid(queryTable.itsJoin) == typeid(stringVariable));
}

BOOST_AUTO_TEST_CASE(querytable_membervariable_initial_value,
                     *boost::unit_test::depends_on("querytable_membervariable_type"))
{
  std::string stringVariable;

  QueryTable queryTable;
  BOOST_CHECK_EQUAL(queryTable.itsName, stringVariable);
  BOOST_CHECK_EQUAL(queryTable.itsAlias, stringVariable);
  // The member variable itsColumns is not initialized by default, can not test it.
  // BOOST_CHECK_EQUAL(queryTable.itsColumns, columnPtrVariable);
  BOOST_CHECK_EQUAL(queryTable.itsJoin, stringVariable);
}

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
