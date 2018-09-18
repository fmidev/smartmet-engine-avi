#include "Utils.h"

#include <spine/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
// ----------------------------------------------------------------------
/*!
 * \brief Get list<string> as 's1,'s2',...
 */
// ----------------------------------------------------------------------

std::string Utils::getStringList(const std::list<std::string>& stringList)
{
  try
  {
    std::string slist;

    for (auto const& s : stringList)
      slist.append(slist.empty() ? "'" : "','").append(s);

    return slist.empty() ? slist : (slist + "'");
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}
}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet
