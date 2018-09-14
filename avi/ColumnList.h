#pragma once

#include "Column.h"

#include <list>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
class ColumnList : public std::list<Column>
{
 public:
  /** \brief Sort columns by their position (in the 'param=' list of the request) */
  void ascendingNumberOrder();
  void descendingNumberOrder();
};
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
