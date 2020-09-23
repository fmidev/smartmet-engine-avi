// ======================================================================

#include "Config.h"
#include <boost/algorithm/string.hpp>
#include <macgyver/StringConversion.h>
#include <macgyver/Exception.h>
#include <stdexcept>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{
// ----------------------------------------------------------------------
/*!
 * \brief The only permitted constructor requires a configfile
 */
// ----------------------------------------------------------------------

Config::Config(const std::string &theConfigFileName) : ConfigBase(theConfigFileName)
{
  try
  {
    const auto &theConfig = get_config();

    // Postgis settings

    if (!theConfig.exists("postgis.host"))
    {
      Fmi::Exception exception(BCP, "Missing configuration attribute!");
      exception.addParameter("Configuration file", theConfigFileName);
      exception.addParameter("Attribute", "postgis.host");
      throw exception;
    }

    if (!theConfig.exists("postgis.port"))
    {
      Fmi::Exception exception(BCP, "Missing configuration attribute!");
      exception.addParameter("Configuration file", theConfigFileName);
      exception.addParameter("Attribute", "postgis.port");
      throw exception;
    }

    if (!theConfig.exists("postgis.database"))
    {
      Fmi::Exception exception(BCP, "Missing configuration attribute!");
      exception.addParameter("Configuration file", theConfigFileName);
      exception.addParameter("Attribute", "postgis.database");
      throw exception;
    }

    if (!theConfig.exists("postgis.username"))
    {
      Fmi::Exception exception(BCP, "Missing configuration attribute!");
      exception.addParameter("Configuration file", theConfigFileName);
      exception.addParameter("Attribute", "postgis.username");
      throw exception;
    }

    if (!theConfig.exists("postgis.password"))
    {
      Fmi::Exception exception(BCP, "Missing configuration attribute!");
      exception.addParameter("Configuration file", theConfigFileName);
      exception.addParameter("Attribute", "postgis.password");
      throw exception;
    }

    if (!theConfig.exists("postgis.encoding"))
    {
      Fmi::Exception exception(BCP, "Missing configuration attribute!");
      exception.addParameter("Configuration file", theConfigFileName);
      exception.addParameter("Attribute", "postgis.encoding");
      throw exception;
    }

    theConfig.lookupValue("postgis.host", itsHost);
    theConfig.lookupValue("postgis.port", itsPort);
    theConfig.lookupValue("postgis.database", itsDatabase);
    theConfig.lookupValue("postgis.username", itsUsername);
    theConfig.lookupValue("postgis.password", itsPassword);
    theConfig.lookupValue("postgis.encoding", itsEncoding);

    // Max # of stations allowed in message query; if <= 0, unlimited

    if (theConfig.exists("message.maxstations"))
      theConfig.lookupValue("message.maxstations", itsMaxMessageStations);
    else
      itsMaxMessageStations = 0;

    // Max # of rows fetched by message query; if <= 0, unlimited

    if (theConfig.exists("message.maxrows"))
      theConfig.lookupValue("message.maxrows", itsMaxMessages);
    else
      itsMaxMessages = 0;

    // Record set's message_time start and end offsets as hours

    if (!theConfig.exists("message.recordsetstarttimeoffsethours"))
    {
      Fmi::Exception exception(BCP, "Missing configuration attribute!");
      exception.addParameter("Configuration file", theConfigFileName);
      exception.addParameter("Attribute", "message.recordsetstarttimeoffsethours");
      throw exception;
    }

    theConfig.lookupValue("message.recordsetstarttimeoffsethours",
                          itsRecordSetStartTimeOffsetHours);

    if (itsRecordSetStartTimeOffsetHours == 0)
    {
      Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
      exception.addDetail("The attribute value must be greater than 0.");
      exception.addParameter("Configuration file", theConfigFileName);
      exception.addParameter("Attribute", "message.recordsetstarttimeoffsethours");
      throw exception;
    }

    if (!theConfig.exists("message.recordsetendtimeoffsethours"))
    {
      Fmi::Exception exception(BCP, "Missing configuration attribute!");
      exception.addParameter("Configuration file", theConfigFileName);
      exception.addParameter("Attribute", "message.recordsetendtimeoffsethours");
      throw exception;
    }

    theConfig.lookupValue("message.recordsetendtimeoffsethours", itsRecordSetEndTimeOffsetHours);

    if (itsRecordSetEndTimeOffsetHours == 0)
    {
      Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
      exception.addDetail("The attribute value must be greater than 0");
      exception.addParameter("Configuration file", theConfigFileName);
      exception.addParameter("Attribute", "message.recordsetendtimeoffsethours");
      throw exception;
    }

    // Filtering of finnish METARs; if true/enabled, by default returning finnish METARs only when
    // they are LIKE "METAR%".
    // Stations can be excluded from filtering by their icao code

    itsFilterFIMETARxxx = get_optional_config_param<bool>(
        theConfig.getRoot(), "message.filter_FI_METARxxx.filter", false);

    if (itsFilterFIMETARxxx)
    {
      if (theConfig.exists("message.filter_FI_METARxxx.excludeicaos"))
      {
        const libconfig::Setting &icaosSetting =
            theConfig.lookup("message.filter_FI_METARxxx.excludeicaos");

        if (!icaosSetting.isArray())
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail("The attribute must contain an array of icao codes.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", "message.filter_FI_METARxxx.excludeicaos");
          throw exception;
        }

        std::string icao;

        for (int j = 0; (j < icaosSetting.getLength()); j++)
        {
          if (icaosSetting[j].getType() != libconfig::Setting::Type::TypeString)
          {
            Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
            exception.addDetail("The attribute must contain an array of strings.");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", "message.filter_FI_METARxxx.excludeicaos");
            throw exception;
          }

          icao = boost::trim_copy(boost::to_upper_copy(std::string((const char *)icaosSetting[j])));

          if (icao.empty())
          {
            Fmi::Exception exception(BCP, "Empty configuration attribute value!");
            exception.addDetail("The attribute value is empty.");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", "message.filter_FI_METARxxx.excludeicaos");
            throw exception;
          }

          if (find(itsFilterFIMETARxxxExcludeIcaos.begin(),
                   itsFilterFIMETARxxxExcludeIcaos.end(),
                   icao) != itsFilterFIMETARxxxExcludeIcaos.end())
          {
            Fmi::Exception exception(
                BCP, "Configuration attribute value contains duplicates!");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", "message.filter_FI_METARxxx.excludeicaos");
            exception.addParameter("Duplicate index", Fmi::to_string(j));
            exception.addParameter("Duplicate value", icao);
            throw exception;
          }

          itsFilterFIMETARxxxExcludeIcaos.push_back(icao);
        }
      }
    }

    // Known message types and settings for querying messages

    if (!theConfig.exists("message.types"))
    {
      Fmi::Exception exception(BCP, "Missing configuration attribute!");
      exception.addParameter("Configuration file", theConfigFileName);
      exception.addParameter("Attribute", "message.types");
      throw exception;
    }

    const libconfig::Setting &messageTypes = theConfig.lookup("message.types");
    std::list<std::string> knownMessageTypes;

    if (!messageTypes.isList())
    {
      Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
      exception.addDetail("The attribute must contain an array of groups.");
      exception.addParameter("Configuration file", theConfigFileName);
      exception.addParameter("Attribute", "message.types");
      throw exception;
    }
    else if (messageTypes.getLength() == 0)
    {
      Fmi::Exception exception(BCP, "Empty configuration attribute value!");
      exception.addDetail("The attribute value is empty.");
      exception.addParameter("Configuration file", theConfigFileName);
      exception.addParameter("Attribute", "message.types");
      throw exception;
    }

    std::string name;

    for (int i = 0; (i < messageTypes.getLength()); i++)
    {
      libconfig::Setting &typeSetting = messageTypes[i];
      std::string blockName("message.types.[" + Fmi::to_string(i) + "]");

      if (!typeSetting.isGroup())
      {
        Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
        exception.addDetail("The attribute must contain an array of groups.");
        exception.addParameter("Configuration file", theConfigFileName);
        exception.addParameter("Attribute", blockName);
        throw exception;
      }

      MessageType messageType;

      // Message type name(s)

      if (theConfig.exists(blockName + ".names"))
      {
        const libconfig::Setting &namesSetting = theConfig.lookup(blockName + ".names");

        if (!namesSetting.isArray())
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail("The attribute must contain an array of names.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", blockName + ".names");
          throw exception;
        }
        else if (namesSetting.getLength() == 0)
        {
          Fmi::Exception exception(BCP, "Empty configuration attribute value!");
          exception.addDetail("The attribute value is empty.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", blockName + ".names");
          throw exception;
        }

        for (int j = 0; (j < namesSetting.getLength()); j++)
        {
          if (namesSetting[j].getType() != libconfig::Setting::Type::TypeString)
          {
            Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
            exception.addDetail("The attribute must contain an array of strings.");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", blockName + ".names");
            throw exception;
          }

          name = boost::trim_copy(boost::to_upper_copy(std::string((const char *)namesSetting[j])));

          if (name.empty())
          {
            Fmi::Exception exception(BCP, "Empty configuration attribute array item!");
            exception.addDetail("The attribute array item is empty.");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", blockName + ".names");
            exception.addParameter("Array index", Fmi::to_string(j));
            throw exception;
          }

          if (find(knownMessageTypes.begin(), knownMessageTypes.end(), name) !=
              knownMessageTypes.end())
          {
            Fmi::Exception exception(
                BCP, "Configuration attribute value contains duplicates!");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", blockName + ".names");
            exception.addParameter("Duplicate index", Fmi::to_string(j));
            exception.addParameter("Duplicate value", name);
            throw exception;
          }

          messageType.addType(name);
          knownMessageTypes.push_back(name);
        }
      }
      else
      {
        name = boost::trim_copy(
            boost::to_upper_copy(get_mandatory_config_param<std::string>(typeSetting, "name")));

        if (name.empty())
        {
          Fmi::Exception exception(BCP, "Empty configuration attribute value!");
          exception.addDetail("The attribute value is empty");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", blockName + ".name");
          throw exception;
        }

        if (find(knownMessageTypes.begin(), knownMessageTypes.end(), name) !=
            knownMessageTypes.end())
        {
          Fmi::Exception exception(BCP, "Configuration redefinition!");
          exception.addDetail("Configuration with the same name is already defined");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", blockName + ".name");
          exception.addParameter("Name", name);
          throw exception;
        }

        messageType.addType(name);
        knownMessageTypes.push_back(name);
      }

      // Message scope

      typedef struct
      {
        const char *scopeName;
        MessageScope messageScope;
      } KnownMessageScope;

      KnownMessageScope messageScopes[] = {{"station", StationScope},
                                           {"fir", FIRScope},
                                           {"global", GlobalScope},
                                           {nullptr, NoScope}};

      KnownMessageScope *s = messageScopes;
      auto scope = get_optional_config_param<std::string>(typeSetting, "scope", "station");

      for (; (s->scopeName); s++)
        if (scope == s->scopeName)
          break;

      if (!(s->scopeName))
      {
        Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
        exception.addDetail(
            "Use one of the following values : \"station\", \"fir\" or \"global\"");
        exception.addParameter("Configuration file", theConfigFileName);
        exception.addParameter("Attribute", blockName + ".scope");
        exception.addParameter("Invalid value", scope);
        throw exception;
      }

      messageType.setScope(s->messageScope);

      // Query time range types (which time columns are used for time restriction).

      typedef struct
      {
        const char *timeRangeName;
        TimeRangeType timeRangeType;
        bool latestMessagesOnly;
      } KnownTimeRangeType;

      KnownTimeRangeType timeRangeTypes[] = {{"validtime", ValidTimeRange, true},
                                             {"messagevalidtime", MessageValidTimeRange, true},
                                             {"messagetime", MessageTimeRange, true},
                                             {"creationtime", CreationValidTimeRange, true},
                                             {nullptr, NullTimeRange, false}};

      KnownTimeRangeType *r = timeRangeTypes;
      std::string timeRangeType =
          boost::trim_copy(get_mandatory_config_param<std::string>(typeSetting, "timerangetype"));

      for (; (r->timeRangeName); r++)
        if (timeRangeType == r->timeRangeName)
          break;

      if (!(r->timeRangeName))
      {
        Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
        exception.addDetail(
            "Use one of the following values : \"validtime\", \"messagetime\" or \"creationtime\"");
        exception.addParameter("Configuration file", theConfigFileName);
        exception.addParameter("Attribute", blockName + ".timerangetype");
        exception.addParameter("Invalid value", timeRangeType);
        throw exception;
      }

      // Validity period length for 'MessageValidTimeRange' and 'MessageTimeRange'

      if ((r->timeRangeType == MessageValidTimeRange) || (r->timeRangeType == MessageTimeRange))
      {
        unsigned int validityHours =
            get_mandatory_config_param<unsigned int>(typeSetting, "validityhours");

        if (validityHours == 0)
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail("The attribute value must be greater than 0.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", blockName + ".validityhours");
          throw exception;
        }

        messageType.setValidityHours(validityHours);
      }

      // Whether to query latest or all messages within the time range

      bool latestMessageOnly;

      if (r->latestMessagesOnly)
      {
        latestMessageOnly = get_mandatory_config_param<bool>(typeSetting, "latestmessage");

        if ((!latestMessageOnly) && (messageType.getMessageTypes().size() > 1))
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail("The attribute value must be 'true' for group of message types.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", blockName + ".latestmessage");
          throw exception;
        }
      }
      else if ((latestMessageOnly =
                    get_optional_config_param<bool>(typeSetting, "latestmessageonly", false)))
      {
        Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
        exception.addDetail("The attribute value cannot be set for the selected time range type.");
        exception.addParameter("Configuration file", theConfigFileName);
        exception.addParameter("Attribute", blockName + ".latestmessageonly");
        throw exception;
      }

      messageType.setLatestMessageOnly(latestMessageOnly);

      messageType.setTimeRangeType(messageType.getLatestMessageOnly()
                                       ? (TimeRangeType)(r->timeRangeType + 1)
                                       : r->timeRangeType);

      // messir_heading LIKE pattern(s) used for additional grouping (e.g. for GAFOR) when querying
      // latest messages

      if (theConfig.exists(blockName + ".messirpatterns"))
      {
        if (messageType.getMessageTypes().size() > 1)
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail("The attribute value cannot be set for group of message types.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", blockName + ".messirpatterns");
          throw exception;
        }

        if (!latestMessageOnly)
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail(
              "The attribute value cannot be set unless 'latestmessage' is 'true'.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", blockName + ".messirpatterns");
          throw exception;
        }

        const libconfig::Setting &patternsSetting = theConfig.lookup(blockName + ".messirpatterns");

        if (!patternsSetting.isArray())
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail("The attribute value must contain an array of patterns.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", blockName + ".messirpatterns");
          throw exception;
        }
        else if (patternsSetting.getLength() == 0)
        {
          Fmi::Exception exception(BCP, "Empty configuration attribute value!");
          exception.addDetail("The attribute value is empty.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", blockName + ".messirpatterns");
          throw exception;
        }

        std::string pattern;

        for (int j = 0; (j < patternsSetting.getLength()); j++)
        {
          if (patternsSetting[j].getType() != libconfig::Setting::Type::TypeString)
          {
            Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
            exception.addDetail("The attribute value must contain an array of strings.");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", blockName + ".messirpatterns");
            throw exception;
          }

          pattern = (const char *)patternsSetting[j];
          boost::trim(pattern);

          if (pattern.empty())
          {
            Fmi::Exception exception(BCP, "Empty configuration attribute array item!");
            exception.addDetail("The attribute array item is empty");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", blockName + ".messirpatterns");
            exception.addParameter("Array index", Fmi::to_string(j));
            throw exception;
          }

          auto const &patterns = messageType.getMessirPatterns();

          if (find(patterns.begin(), patterns.end(), pattern) != patterns.end())
          {
            Fmi::Exception exception(
                BCP, "Configuration attribute value contains duplicates!");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", blockName + ".messirpatterns");
            exception.addParameter("Duplicate index", Fmi::to_string(j));
            exception.addParameter("Duplicate value", pattern);
            throw exception;
          }

          messageType.addMessirPattern(pattern);
        }
      }

      itsMessageTypes.push_back(messageType);
    }

    itsFilterFIMETARxxx = (itsFilterFIMETARxxx &&
                           (find(knownMessageTypes.begin(), knownMessageTypes.end(), "METAR") !=
                            knownMessageTypes.end()));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Configuration failed!");
  }
}

}  // namespace Avi
}  // namespace Engine
}  // namespace SmartMet

// ======================================================================
