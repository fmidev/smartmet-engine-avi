#include "ColumnList.h"

#include <spine/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
void ColumnList::ascendingNumberOrder()
{
  try
  {
    sort();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP,
                                            "ColumnList element sort in ascending order failed.");
  }
}
void ColumnList::descendingNumberOrder()
{
  try
  {
    sort(ColumnNumberGreaterComparator());
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP,
                                            "ColumnList element sort in descending order failed.");
  }
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
