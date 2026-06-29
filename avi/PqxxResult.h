// ======================================================================
/*!
 * \brief IResult backend wrapping a libpqxx result
 *
 * Lets loadQueryResult() / StationQueryData::getValues() parse PostgreSQL
 * query results through the same backend-agnostic interface (ResultAccessor.h)
 * used for the DuckDB cache. pqxx::result is reference counted, so holding a
 * copy is cheap.
 */
// ======================================================================

#pragma once

#include "ResultAccessor.h"
#include <pqxx/result>
#include <string>
#include <utility>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
class PqxxResult : public IResult
{
 public:
  explicit PqxxResult(pqxx::result result) : itsResult(std::move(result)) {}

  std::size_t size() const override { return itsResult.size(); }

  bool isNull(std::size_t row, const std::string& column) const override
  {
    return itsResult.at(row)[column].is_null();
  }
  int getInt(std::size_t row, const std::string& column) const override
  {
    return itsResult.at(row)[column].as<int>();
  }
  long getLong(std::size_t row, const std::string& column) const override
  {
    return itsResult.at(row)[column].as<long>();
  }
  double getDouble(std::size_t row, const std::string& column) const override
  {
    return itsResult.at(row)[column].as<double>();
  }
  std::string getString(std::size_t row, const std::string& column) const override
  {
    return itsResult.at(row)[column].as<std::string>();
  }

 private:
  pqxx::result itsResult;
};

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet

// ======================================================================
