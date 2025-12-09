// ======================================================================

#include "Config.h"
#include <boost/algorithm/string.hpp>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <set>
#include <stdexcept>

namespace SmartMet
{
namespace Engine
{
namespace Avi
{

Config::~Config() = default;

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

    if (theConfig.exists("postgis.startconnections"))
    {
      theConfig.lookupValue("postgis.startconnections", startConnections);
    }

    if (theConfig.exists("postgis.maxconnections"))
    {
      theConfig.lookupValue("postgis.maxconnections", maxConnections);
      maxConnections = std::max(startConnections, maxConnections);
    }

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
            Fmi::Exception exception(BCP, "Configuration attribute value contains duplicates!");
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
            Fmi::Exception exception(BCP, "Configuration attribute value contains duplicates!");
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

      using KnownMessageScope = struct
      {
        const char *scopeName;
        MessageScope messageScope;
      };

      KnownMessageScope messageScopes[] = {{"station", MessageScope::StationScope},
                                           {"fir", MessageScope::FIRScope},
                                           {"global", MessageScope::GlobalScope},
                                           {nullptr, MessageScope::NoScope}};

      KnownMessageScope *s = messageScopes;
      auto scope = get_optional_config_param<std::string>(typeSetting, "scope", "station");

      for (; (s->scopeName); s++)
        if (scope == s->scopeName)
          break;

      if (!(s->scopeName))
      {
        Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
        exception.addDetail(R"(Use one of the following values : "station", "fir" or "global")");
        exception.addParameter("Configuration file", theConfigFileName);
        exception.addParameter("Attribute", blockName + ".scope");
        exception.addParameter("Invalid value", scope);
        throw exception;
      }

      messageType.setScope(s->messageScope);

      // Query time range types (which time columns are used for time restriction).

      using KnownTimeRangeType = struct
      {
        const char *timeRangeName;
        TimeRangeType timeRangeType;
        bool latestMessagesOnly;
      };

      KnownTimeRangeType timeRangeTypes[] = {
          {"validtime", TimeRangeType::ValidTimeRange, true},
          {"messagevalidtime", TimeRangeType::MessageValidTimeRange, true},
          {"messagetime", TimeRangeType::MessageTimeRange, true},
          {"creationtime", TimeRangeType::CreationValidTimeRange, true},
          {nullptr, TimeRangeType::NullTimeRange, false}};

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
            R"(Use one of the following values : "validtime", "messagetime" or "creationtime")");
        exception.addParameter("Configuration file", theConfigFileName);
        exception.addParameter("Attribute", blockName + ".timerangetype");
        exception.addParameter("Invalid value", timeRangeType);
        throw exception;
      }

      // Validity period length for 'MessageValidTimeRange' and 'MessageTimeRange'

      if ((r->timeRangeType == TimeRangeType::MessageValidTimeRange) ||
          (r->timeRangeType == TimeRangeType::MessageTimeRange))
      {
        auto validityHours = get_mandatory_config_param<unsigned int>(typeSetting, "validityhours");

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
      else
      {
        latestMessageOnly = get_optional_config_param<bool>(typeSetting, "latestmessage", false);
        if (latestMessageOnly)
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail(
              "The attribute value cannot be set for the selected time range type.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", blockName + ".latestmessage");
          throw exception;
        }
      }

      messageType.setLatestMessageOnly(latestMessageOnly);

      messageType.setTimeRangeType(messageType.getLatestMessageOnly()
                                       ? TimeRangeType(static_cast<int>(r->timeRangeType) + 1)
                                       : r->timeRangeType);

      // messir_heading LIKE pattern(s) used for additional grouping (e.g. for GAFOR) when querying
      // latest messages

      std::string attrBlockName(blockName + ".messirpatterns");

      if (theConfig.exists(attrBlockName))
      {
        if (messageType.getMessageTypes().size() > 1)
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail("The attribute value cannot be set for group of message types.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", attrBlockName);
          throw exception;
        }

        if (!latestMessageOnly)
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail(
              "The attribute value cannot be set unless 'latestmessage' is 'true'.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", attrBlockName);
          throw exception;
        }

        const libconfig::Setting &patternsSetting = theConfig.lookup(attrBlockName);

        if (!patternsSetting.isArray())
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail("The attribute value must contain an array of patterns.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", attrBlockName);
          throw exception;
        }
        else if (patternsSetting.getLength() == 0)
        {
          Fmi::Exception exception(BCP, "Empty configuration attribute value!");
          exception.addDetail("The attribute value is empty.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", attrBlockName);
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
            exception.addParameter("Attribute", attrBlockName);
            throw exception;
          }

          pattern = (const char *)patternsSetting[j];
          boost::trim(pattern);

          if (pattern.empty())
          {
            Fmi::Exception exception(BCP, "Empty configuration attribute array item!");
            exception.addDetail("The attribute array item is empty");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", attrBlockName);
            exception.addParameter("Array index", Fmi::to_string(j));
            throw exception;
          }

          auto const &patterns = messageType.getMessirPatterns();

          if (find(patterns.begin(), patterns.end(), pattern) != patterns.end())
          {
            Fmi::Exception exception(BCP, "Configuration attribute value contains duplicates!");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", attrBlockName);
            exception.addParameter("Duplicate index", Fmi::to_string(j));
            exception.addParameter("Duplicate value", pattern);
            throw exception;
          }

          messageType.addMessirPattern(pattern);
        }
      }

      // Message (i.e. TAF) query restriction hours, icao patterns, country codes
      // and starting/ending minutes
      //
      // TAFs are stored e.g. every n'th (3rd) hour between xx:20 and xx:40 and then
      // published; during publication hour output of latest message is delayed until xx:40
      //

      attrBlockName = blockName + ".queryrestrictionhours";

      if (theConfig.exists(attrBlockName))
      {
        if (!latestMessageOnly)
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail(
              "The attribute value cannot be set unless 'latestmessage' is 'true'.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", attrBlockName);
          throw exception;
        }

        if (messageType.getTimeRangeType() != TimeRangeType::MessageValidTimeRangeLatest)
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail(
              "The attribute value cannot be set unless 'timerangetype' is 'messagevalidtime'");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", attrBlockName);
          throw exception;
        }

        const libconfig::Setting &hoursSetting = theConfig.lookup(attrBlockName);

        if (!hoursSetting.isArray())
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail("The attribute value must contain an array of hour values (0-23).");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", attrBlockName);
          throw exception;
        }
        else if (hoursSetting.getLength() == 0)
        {
          Fmi::Exception exception(BCP, "Empty configuration attribute value!");
          exception.addDetail("The attribute value is empty.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", attrBlockName);
          throw exception;
        }

        std::set<int> restrictionHours;

        for (int j = 0; (j < hoursSetting.getLength()); j++)
        {
          if (hoursSetting[j].getType() != libconfig::Setting::Type::TypeInt)
          {
            Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
            exception.addDetail("The attribute value must contain an array of hour values (0-23).");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", attrBlockName);
            throw exception;
          }

          int hour = hoursSetting[j];

          if ((hour < 0) || (hour > 23))
          {
            Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
            exception.addDetail("The attribute value is invalid, hour 0-23 expected");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", attrBlockName);
            exception.addParameter("Array index", Fmi::to_string(j));
            throw exception;
          }

          if (restrictionHours.find(hour) != restrictionHours.end())
          {
            Fmi::Exception exception(BCP, "Configuration attribute value contains duplicates!");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", attrBlockName);
            exception.addParameter("Duplicate index", Fmi::to_string(j));
            exception.addParameter("Duplicate value", Fmi::to_string(hour));
            throw exception;
          }

          restrictionHours.insert(hour);
        }

        std::string hourList;

        for (auto rh = restrictionHours.cbegin(); rh != restrictionHours.cend(); rh++)
          hourList += (((rh != restrictionHours.cbegin()) ? "," : "") + Fmi::to_string(*rh));

        messageType.setQueryRestrictionHours(hourList);
      }

      // Query restriction icao LIKE pattern(s) and country code(s)

      attrBlockName = blockName + ".queryrestrictionicaopatterns";
      bool hasPatterns = theConfig.exists(attrBlockName);

      std::string codeAttrBlockName = blockName + ".queryrestrictioncountrycodes";
      bool hasCodes = theConfig.exists(codeAttrBlockName);

      if (((!messageType.getQueryRestrictionHours().empty()) && (!hasCodes)) || hasPatterns)
      {
        if (messageType.getQueryRestrictionHours().empty())
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail(
              "The attribute value cannot be set unless 'queryrestrictionhours' is set");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", attrBlockName);
          throw exception;
        }
        else if (!hasPatterns)
        {
          Fmi::Exception exception(BCP, "Missing configuration attribute value!");
          exception.addDetail(
              "The attribute value must be set when 'queryrestrictionhours' is set");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", attrBlockName);
          throw exception;
        }

        const libconfig::Setting &patternsSetting = theConfig.lookup(attrBlockName);

        if (!patternsSetting.isArray())
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail("The attribute value must contain an array of patterns.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", attrBlockName);
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
            exception.addParameter("Attribute", attrBlockName);
            throw exception;
          }

          pattern = Fmi::ascii_toupper_copy((const char *)patternsSetting[j]);
          boost::trim(pattern);

          if (pattern.empty())
          {
            Fmi::Exception exception(BCP, "Empty configuration attribute array item!");
            exception.addDetail("The attribute array item is empty");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", attrBlockName);
            exception.addParameter("Array index", Fmi::to_string(j));
            throw exception;
          }

          auto const &patterns = messageType.getQueryRestrictionIcaoPatterns();

          if (find(patterns.begin(), patterns.end(), pattern) != patterns.end())
          {
            Fmi::Exception exception(BCP, "Configuration attribute value contains duplicates!");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", attrBlockName);
            exception.addParameter("Duplicate index", Fmi::to_string(j));
            exception.addParameter("Duplicate value", pattern);
            throw exception;
          }

          messageType.addQueryRestrictionIcaoPattern(pattern);
        }
      }

      if (((!messageType.getQueryRestrictionHours().empty()) && (!hasPatterns)) || hasCodes)
      {
        if (messageType.getQueryRestrictionHours().empty())
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail(
              "The attribute value cannot be set unless 'queryrestrictionhours' is set");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", codeAttrBlockName);
          throw exception;
        }
        else if (!hasCodes)
        {
          Fmi::Exception exception(BCP, "Missing configuration attribute value!");
          exception.addDetail(
              "The attribute value must be set when 'queryrestrictionhours' is set");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", codeAttrBlockName);
          throw exception;
        }

        const libconfig::Setting &codesSetting = theConfig.lookup(codeAttrBlockName);

        if (!codesSetting.isArray())
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail("The attribute value must contain an array of country codes.");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", codeAttrBlockName);
          throw exception;
        }

        std::string code;

        for (int j = 0; (j < codesSetting.getLength()); j++)
        {
          if (codesSetting[j].getType() != libconfig::Setting::Type::TypeString)
          {
            Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
            exception.addDetail("The attribute value must contain an array of strings.");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", codeAttrBlockName);
            throw exception;
          }

          code = Fmi::ascii_toupper_copy((const char *)codesSetting[j]);
          boost::trim(code);

          if (code.empty())
          {
            Fmi::Exception exception(BCP, "Empty configuration attribute array item!");
            exception.addDetail("The attribute array item is empty");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", codeAttrBlockName);
            exception.addParameter("Array index", Fmi::to_string(j));
            throw exception;
          }

          auto const &codes = messageType.getQueryRestrictionCountryCodes();

          if (find(codes.begin(), codes.end(), code) != codes.end())
          {
            Fmi::Exception exception(BCP, "Configuration attribute value contains duplicates!");
            exception.addParameter("Configuration file", theConfigFileName);
            exception.addParameter("Attribute", codeAttrBlockName);
            exception.addParameter("Duplicate index", Fmi::to_string(j));
            exception.addParameter("Duplicate value", code);
            throw exception;
          }

          messageType.addQueryRestrictionCountryCode(code);
        }
      }

      if ((!messageType.getQueryRestrictionHours().empty()) &&
          messageType.getQueryRestrictionIcaoPatterns().empty() &&
          messageType.getQueryRestrictionCountryCodes().empty())
      {
        Fmi::Exception exception(BCP,
                                 "Query restriction icao pattern(s) and country code(s) missing!");
        exception.addParameter("Configuration file", theConfigFileName);
        exception.addParameter("Attribute", blockName);
        throw exception;
      }

      // Query restriction starting and ending minute

      std::string startBlockName(blockName + ".queryrestrictionstartminute");
      std::string endBlockName(blockName + ".queryrestrictionendminute");
      bool hasStart = theConfig.exists(startBlockName);
      bool hasEnd = theConfig.exists(endBlockName);

      if ((!messageType.getQueryRestrictionHours().empty()) || hasStart || hasEnd)
      {
        if (messageType.getQueryRestrictionHours().empty())
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail(
              "The attribute value cannot be set unless 'queryrestrictionhours' is set");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", hasStart ? startBlockName : endBlockName);
          throw exception;
        }
        else if ((!hasStart) || (!hasEnd))
        {
          Fmi::Exception exception(BCP, "Missing configuration attribute value!");
          exception.addDetail(
              "The attribute value must be set when 'queryrestrictionhours' is set");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", hasStart ? endBlockName : startBlockName);
          throw exception;
        }

        const libconfig::Setting &startMinuteSetting = theConfig.lookup(startBlockName);
        const libconfig::Setting &endMinuteSetting = theConfig.lookup(endBlockName);
        bool startMinuteOk = (startMinuteSetting.getType() == libconfig::Setting::Type::TypeInt);
        bool endMinuteOk = (endMinuteSetting.getType() == libconfig::Setting::Type::TypeInt);
        int startMinute = 0, endMinute = 0;

        if (startMinuteOk && endMinuteOk)
        {
          startMinute = startMinuteSetting;
          endMinute = endMinuteSetting;
          startMinuteOk = ((startMinute >= 0) && (startMinute <= 59));
          endMinuteOk = ((endMinute > startMinute) && (endMinute <= 59));
        }

        if ((!startMinuteOk) || (!endMinuteOk))
        {
          Fmi::Exception exception(BCP, "Invalid configuration attribute value!");
          exception.addDetail("The attribute value must contain minute value (0-59).");
          exception.addParameter("Configuration file", theConfigFileName);
          exception.addParameter("Attribute", startMinuteOk ? endBlockName : startBlockName);
          throw exception;
        }

        messageType.setQueryRestrictionStartMinute(startMinute);
        messageType.setQueryRestrictionEndMinute(endMinute);
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
