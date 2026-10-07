# smartmet-engine-avi

Part of [SmartMet Server](https://github.com/fmidev/smartmet-server). See the [SmartMet Server documentation](https://github.com/fmidev/smartmet-server) for a full overview of the ecosystem.

## Overview

The AVI engine provides access to aviation weather data — METAR (airport observations), TAF (terminal aerodrome forecasts), and SIGMET (significant meteorological information) — for use by SmartMet Server plugins.

## Running without a database (dummy mode)

The engine can be loaded in a dummy mode that needs no database. In this mode it starts normally, so plugins that look up the Avi engine still initialize, but every query method throws `AVI engine not available`. Dummy mode is enabled in either of two ways:

1. Leave the engine's configuration file unspecified or set it to an empty string in the server configuration:

   ```
   engines:
   {
     avi:
     {
       configfile = "";   # or omit configfile entirely
     };
   };
   ```

2. Give a configuration file that sets the engine's own disable flag. No other settings are read, so this line alone is enough:

   ```
   disabled = true;
   ```

Setting `disabled = true` in the server's `engines.avi` section is different: the server then does not load `avi.so` at all, and any plugin that requires the Avi engine fails at startup.

## Documentation

- [Developer guide](docs/developer-guide.md) — API, query building, message types and time ranges, configuration, pitfalls

## License

MIT — see [LICENSE](LICENSE)

## Contributing

Bug reports and pull requests are welcome on [GitHub](../../issues).
