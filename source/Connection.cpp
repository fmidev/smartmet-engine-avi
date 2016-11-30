#include "Connection.h"

#include <spine/Exception.h>

#include <boost/foreach.hpp>
#include <boost/make_shared.hpp>
#include <iostream>

using namespace std;

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
Connection::Connection(const std::string& theHost,
                       int thePort,
                       const std::string& theUser,
                       const std::string& thePassword,
                       const std::string& theDatabase,
                       const std::string& theClientEncoding,
                       bool theDebug /* = false */)
    : itsCollateSupported(false), itsDebug(theDebug)
{
  try
  {
    open(theHost, thePort, theUser, thePassword, theDatabase, theClientEncoding);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool Connection::open(const std::string& theHost,
                      int thePort,
                      const std::string& theUser,
                      const std::string& thePassword,
                      const std::string& theDatabase,
                      const std::string& theClientEncoding)
{
  try
  {
    std::stringstream ss;
    (void)theClientEncoding;

    ss << "host=" << theHost << " port=" << thePort << " dbname=" << theDatabase
       << " user=" << theUser << " password=" << thePassword
#if 0
         << " client_encoding="
         << theClientEncoding
#endif
        ;

    try
    {
      itsConnection = boost::make_shared<pqxx::connection>(ss.str());
      /*
        pqxx::result res = executeNonTransaction("SELECT version()");

        if(PostgreSQL > 9.1)
                itsCollateSupported = true;
      */
    }
    catch (...)
    {
      SmartMet::Spine::Exception exception(BCP, "Connection to the database failed!", NULL);
      exception.addParameter("Database", theDatabase);
      exception.addParameter("User", theUser);
      throw exception;
    }

    return itsConnection->is_open();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Connection::close()
{
  try
  {
    // disconnect() does not throw according to documentation

    itsConnection->disconnect();

#if 0
      try {
        if (itsConnection->is_open())
          itsConnection->disconnect();
      }
      catch (const std::exception& e) {
        throw SmartMet::Spine::Exception(BCP,string("Failed to disconnect database: ") + e.what());
      }
#endif
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

pqxx::result Connection::executeNonTransaction(const std::string& theSQLStatement) const
{
  try
  {
    if (itsDebug)
      std::cout << "SQL: " << theSQLStatement << std::endl;

    try
    {
      pqxx::nontransaction ntrx(*itsConnection);
      return ntrx.exec(theSQLStatement);
    }
    catch (...)
    {
      SmartMet::Spine::Exception exception(BCP, "Execution of the SQL statement failed!", NULL);
      exception.addParameter("SQL statement", theSQLStatement);
      throw exception;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet

// ======================================================================
