// ======================================================================
/*!
 * \brief Implementation of class ConnectionPool
 */
// ======================================================================

#include "ConnectionPool.h"
#include <fmt/format.h>
#include <macgyver/Exception.h>
#include <spine/Convenience.h>
#include <iostream>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
ConnectionPool::Ptr::Ptr(ConnectionPool& thePool,
                         Fmi::Database::PostgreSQLConnectionPool::Ptr thePtr)
    : itsPool(thePool), itsPtr(std::move(thePtr))
{
  // If recycling throws, itsPtr's destructor returns the connection to the pool; its old
  // release time is not touched, so recycling is retried on the next checkout
  itsPool.recycleIfIdle(itsPtr.get());
}

ConnectionPool::Ptr::~Ptr()
{
  // Moved-from handles hold no connection
  if (itsPtr)
    itsPool.noteReleased(itsPtr.get());
}

ConnectionPool::ConnectionPool(std::size_t startConnections,
                               std::size_t maxConnections,
                               const Fmi::Database::PostgreSQLConnectionOptions& options,
                               std::chrono::seconds maxIdleTime)
try : itsPool(startConnections, maxConnections, options),
      itsMaxIdleTime(maxIdleTime),
      itsCreationTime(std::chrono::steady_clock::now())
{
}
catch (...)
{
  throw Fmi::Exception::Trace(BCP, "Failed to create PostgreSQL connection pool");
}

ConnectionPool::Ptr ConnectionPool::get()
{
  return {*this, itsPool.get()};
}

// ----------------------------------------------------------------------
/*!
 * \brief Close and reopen a connection idle longer than the max idle time
 *
 * Rather than trying to detect whether the connection is still alive, it is unconditionally
 * reopened once idle long enough. reopen() reconnects synchronously (bounded by the usual
 * connect retries and timeout) on a new socket.
 */
// ----------------------------------------------------------------------

void ConnectionPool::recycleIfIdle(Fmi::Database::PostgreSQLConnection* connection)
{
  if (itsMaxIdleTime.count() <= 0 || !connection)
    return;

  try
  {
    // A connection not released yet has been idle since the pool was created (the pool opens
    // its start connections then). This also applies to a connection the pool opens later when
    // it grows, which may then be reopened once unnecessarily; that cannot be told apart.
    auto idleSince = itsCreationTime;

    {
      const std::lock_guard<std::mutex> lock(itsMutex);
      auto it = itsReleaseTimes.find(connection);
      if (it != itsReleaseTimes.end())
        idleSince = it->second;
    }

    const auto idleTime = std::chrono::steady_clock::now() - idleSince;
    if (idleTime <= itsMaxIdleTime)
      return;

    if (!connection->reopen())
      throw Fmi::Exception(BCP, "Failed to reopen idle PostgreSQL connection");

    std::cout << fmt::format(
        "{} AVI: reopened PostgreSQL connection to {} idle for {:.1f} s (postgis.maxidletime {} s)\n",
        Spine::log_time_str(),
        connection->getId(),
        std::chrono::duration<double>(idleTime).count(),
        itsMaxIdleTime.count());
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failed to recycle idle PostgreSQL connection");
  }
}

void ConnectionPool::noteReleased(Fmi::Database::PostgreSQLConnection* connection)
{
  if (itsMaxIdleTime.count() <= 0 || !connection)
    return;

  const std::lock_guard<std::mutex> lock(itsMutex);
  itsReleaseTimes[connection] = std::chrono::steady_clock::now();
}

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
