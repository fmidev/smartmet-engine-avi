// ======================================================================
/*!
 * \brief Backend-agnostic query result accessor
 *
 * The AVI engine generates PostgreSQL query text and (historically) executed
 * it against libpqxx, parsing the pqxx::result directly. To allow the very
 * same generated SQL to be executed against an in-process DuckDB mirror of the
 * recent messages (see AviCache), result parsing is expressed against this
 * minimal abstraction instead of pqxx::result.
 *
 * IResult exposes column access by name + row index. Two backends implement
 * it: a libpqxx backend (PqxxResult, defined in EngineImpl.cpp where pqxx
 * results are produced) and a DuckDB backend (DuckResult, defined in
 * AviCache.cpp). This header is intentionally free of pqxx/duckdb includes so
 * that the public Engine.h can depend on it without leaking either library.
 */
// ======================================================================

#pragma once

#include <cstddef>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
class IResult
{
 public:
  IResult() = default;
  virtual ~IResult() = default;
  IResult(const IResult&) = delete;
  IResult& operator=(const IResult&) = delete;
  IResult(IResult&&) = delete;
  IResult& operator=(IResult&&) = delete;

  // Number of rows in the result
  virtual std::size_t size() const = 0;

  // Column access by name for the given row. Implementations look the column
  // up by name (as the previous pqxx-based code did). getString returns the
  // raw textual value; the caller trims and/or parses it (e.g. timestamps are
  // returned as text and fed to Fmi::DateTime::from_string).
  virtual bool isNull(std::size_t row, const std::string& column) const = 0;
  virtual int getInt(std::size_t row, const std::string& column) const = 0;
  virtual long getLong(std::size_t row, const std::string& column) const = 0;
  virtual double getDouble(std::size_t row, const std::string& column) const = 0;
  virtual std::string getString(std::size_t row, const std::string& column) const = 0;
};

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet

// ======================================================================
