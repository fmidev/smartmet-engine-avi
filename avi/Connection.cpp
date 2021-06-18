#include "Connection.h"

#include <macgyver/Exception.h>

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
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
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
      throw Fmi::Exception::Trace(BCP, "Connection to the database failed!")
          .addParameter("Database", theDatabase)
          .addParameter("User", theUser);
    }

    return itsConnection->is_open();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void Connection::close()
{
  try
  {

#if PQXX_VERSION_MAJOR < 7
    itsConnection->disconnect();
#else
      try {
        if (itsConnection->is_open())
          itsConnection->close();
      }
      catch (const std::exception& e) {
        throw Fmi::Exception(BCP,string("Failed to disconnect database: ") + e.what());
      }
#endif
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
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
    catch (const std::exception& e)
    {
      throw Fmi::Exception(BCP, e.what()).addParameter("SQL statement", theSQLStatement);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet

// ======================================================================
