// ======================================================================
/*!
 * \brief PostgreSQL connection pool recycling connections idle too long
 *
 * Wraps Fmi::Database::PostgreSQLConnectionPool. A connection that has
 * been idle in the pool longer than the configured max idle time is closed
 * and reopened when it is checked out, before it is handed to the caller.
 *
 * This is needed because PostgreSQLConnection::isConnected() only reflects
 * pqxx's local is_open() flag: it does not detect a connection that
 * something stateful on the network path (firewall, NAT, ...) silently
 * dropped (no FIN/RST) while it sat idle in the pool. A query sent on such
 * a connection would otherwise stall until the kernel gives up retransmitting
 * (tcp_retries2, typically 15-20 minutes) unless tcp_user_timeout is set.
 */
// ======================================================================

#pragma once

#include <macgyver/PostgreSQLConnection.h>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <unordered_map>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
class ConnectionPool
{
 public:
  // RAII handle to a checked out connection. Records the release time and
  // returns the connection to the pool on destruction.
  class Ptr
  {
   public:
    Ptr(ConnectionPool& thePool, Fmi::Database::PostgreSQLConnectionPool::Ptr thePtr);
    ~Ptr();

    Ptr(Ptr&& other) = default;
    Ptr& operator=(Ptr&&) = delete;
    Ptr(const Ptr&) = delete;
    Ptr& operator=(const Ptr&) = delete;

    Fmi::Database::PostgreSQLConnection* get() const { return itsPtr.get(); }
    Fmi::Database::PostgreSQLConnection* operator->() const { return itsPtr.get(); }
    Fmi::Database::PostgreSQLConnection& operator*() const { return *itsPtr.get(); }

   private:
    ConnectionPool& itsPool;
    Fmi::Database::PostgreSQLConnectionPool::Ptr itsPtr;
  };

  // maxIdleTime 0 disables idle connection recycling
  ConnectionPool(std::size_t startConnections,
                 std::size_t maxConnections,
                 const Fmi::Database::PostgreSQLConnectionOptions& options,
                 std::chrono::seconds maxIdleTime = std::chrono::seconds(0));

  ConnectionPool(const ConnectionPool&) = delete;
  ConnectionPool& operator=(const ConnectionPool&) = delete;

  Ptr get();

 private:
  void recycleIfIdle(Fmi::Database::PostgreSQLConnection* connection);
  void noteReleased(Fmi::Database::PostgreSQLConnection* connection);

  Fmi::Database::PostgreSQLConnectionPool itsPool;
  const std::chrono::seconds itsMaxIdleTime;

  // Idle time reference for connections not released yet, i.e. opened when the pool was
  // created and not used since
  const std::chrono::steady_clock::time_point itsCreationTime;

  // Release times of the connections currently idle in the pool. The pool never destroys
  // its connections before the pool itself, so the pointers stay valid as keys.
  std::mutex itsMutex;
  std::unordered_map<Fmi::Database::PostgreSQLConnection*, std::chrono::steady_clock::time_point>
      itsReleaseTimes;
};

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
