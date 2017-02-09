// ======================================================================

#pragma once

#include <string>
#include <boost/shared_ptr.hpp>
#include <pqxx/pqxx>
#include <list>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
class Connection
{
 public:
  Connection(const std::string &theHost,
             int thePort,
             const std::string &theUser,
             const std::string &thePassword,
             const std::string &theDatabase,
             const std::string &theClientEncoding,
             bool theDebug = false);
  Connection() = delete;
  Connection(const Connection &) = delete;
  Connection &operator=(const Connection &) = delete;
  ~Connection() { close(); }
  bool isConnected() const { return itsConnection->is_open(); }
  bool collateSupported() { return itsCollateSupported; }
  void setDebug(bool theDebug) { itsDebug = theDebug; }
  std::string quote(const std::string &theString) const { return "'" + theString + "'"; }
  pqxx::result executeNonTransaction(const std::string &theSQLStatement) const;

 private:
  bool open(const std::string &theHost,
            int thePort,
            const std::string &theUser,
            const std::string &thePassword,
            const std::string &theDatabase,
            const std::string &theClientEncoding);
  void close();

  boost::shared_ptr<pqxx::connection> itsConnection;
  bool itsCollateSupported;
  bool itsDebug;

};  // class Connection

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet

// ======================================================================
