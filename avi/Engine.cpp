#include "Engine.h"

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
Engine::~Engine() = default;

std::ostream &operator<<(std::ostream &os, Validity v)
{
  switch (v)
  {
    case Validity::Accepted:
      return os << "Accepted";
    case Validity::Rejected:
      return os << "Rejected";
    case Validity::AcceptedMessages:
      return os << "AcceptedMessages";
    default:
      return os << "???";
  }
}

std::ostream &operator<<(std::ostream &os, ColumnType t)
{
  switch (t)
  {
    case ColumnType::Integer:
      return os << "Integer";
    case ColumnType::Double:
      return os << "Double";
    case ColumnType::String:
      return os << "String";
    case ColumnType::TS_LonLat:
      return os << "TS_LonLat";
    case ColumnType::TS_LatLon:
      return os << "TS_LatLon";
    case ColumnType::DateTime:
      return os << "DateTime";
    case ColumnType::None:
      return os << "None";
    default:
      return os << "???";
  }
}

std::ostream &operator<<(std::ostream &os, ColumnSelection s)
{
  switch (s)
  {
    case ColumnSelection::Requested:
      return os << "Requested";
    case ColumnSelection::Automatic:
      return os << "Automatic";
    case ColumnSelection::AutomaticRequested:
      return os << "AutomaticRequested";
    case ColumnSelection::AutomaticOnly:
      return os << "AutomaticOnly";
    default:
      return os << "???";
  }
}

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
