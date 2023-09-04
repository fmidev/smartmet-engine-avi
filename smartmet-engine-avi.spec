%define DIRNAME avi
%define LIBNAME smartmet-%{DIRNAME}
%define SPECNAME smartmet-engine-%{DIRNAME}
Summary: SmartMet aviation message engine
Name: %{SPECNAME}
Version: 23.7.28
Release: 1%{?dist}.fmi
License: FMI
Group: SmartMet/Engines
URL: https://github.com/fmidev/smartmet-engine-avi
Source0: %{name}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%if 0%{?rhel} && 0%{rhel} < 9
%define smartmet_boost boost169
%else
%define smartmet_boost boost
%endif

BuildRequires: rpm-build
BuildRequires: gcc-c++
BuildRequires: make
BuildRequires: %{smartmet_boost}-devel
BuildRequires: zlib-devel
BuildRequires: bzip2-devel
BuildRequires: smartmet-library-spine-devel >= 23.7.28
BuildRequires: smartmet-library-macgyver-devel >= 23.7.28
BuildRequires: smartmet-library-timeseries-devel >= 23.7.28
Requires: %{smartmet_boost}-date-time
Requires: smartmet-library-macgyver >= 23.7.28
Requires: smartmet-library-spine >= 23.7.28
Requires: smartmet-library-timeseries >= 23.7.28
#TestRequires: smartmet-library-spine-plugin-test
#TestRequires: smartmet-test-db
#TestRequires: smartmet-utils-deve
#TestRequires: smartmet-library-spine-devel >= 23.7.28

%if %{defined el7}
Requires: libpqxx < 1:7.0
BuildRequires: libpqxx-devel < 1:7.0
%else
%if 0%{?rhel} && 0%{rhel} >= 8
Requires: libpqxx >= 1:7.7.0, libpqxx < 1:7.8.0
BuildRequires: libpqxx-devel >= 1:7.7.0, libpqxx-devel < 1:7.8.0
#TestRequires: libpqxx-devel >= 1:7.7.0, libpqxx-devel < 1:7.8.0
%else
Requires: libpqxx
BuildRequires: libpqxx-devel
%endif
%endif

Provides: %{SPECNAME}
Obsoletes: smartmet-brainstorm-aviengine < 16.11.1
Obsoletes: smartmet-brainstorm-aviengine-debuginfo < 16.11.1

%description
SmartMet aviation message engine

%package -n %{SPECNAME}-devel
Summary: SmartMet %{SPECNAME} development headers
Group: SmartMet/Development
Provides: %{SPECNAME}-devel
Requires: %{SPECNAME} = %version-%release
Obsoletes: smartmet-brainstorm-aviengine-devel < 16.11.1
%description -n %{SPECNAME}-devel
SmartMet %{SPECNAME} development headers.

%prep

%setup -q -n %{SPECNAME}
 
%build -q -n %{SPECNAME}
make %{_smp_mflags}

%install
%makeinstall

%clean
# rm -rf $RPM_BUILD_ROOT

%files -n %{SPECNAME}
%defattr(0755,root,root,0775)
%{_datadir}/smartmet/engines/%{DIRNAME}.so

%files -n %{SPECNAME}-devel
%defattr(0664,root,root,0775)
%{_includedir}/smartmet/engines/%{DIRNAME}

%changelog
* Fri Jul 28 2023 Andris Pavēnis <andris.pavenis@fmi.fi> 23.7.28-1.fmi
- Repackage due to bulk ABI changes in macgyver/newbase/spine

* Wed Jun 14 2023 Pertti Kinnia <pertti.kinnia@fmi.fi> 23.6.14-1.fmi
- Using UPPER(requesticao) when validating request icao codes against station table; UPPER() was left off when code was changed to use parametrized sql

* Wed May 31 2023 Pertti Kinnia <pertti.kinnia@fmi.fi> 23.5.31-1.fmi
- Using parametrized SQL to validate user input (BRAINSTORM-2010)
- Disabled some deprecated/invalid tests and some test results have changed (new stations added)

* Mon Feb 20 2023 Andris Pavēnis <andris.pavenis@fmi.fi> 23.2.20-1.fmi
- SQL generation update (use PostgreSQLConnection::quote)

* Fri Dec 16 2022 Mika Heiskanen <mika.heiskanen@fmi.fi> - 22.12.16-1.fmi
- Repackaged since PostgreSQLConnection ABI changed

* Thu Oct  6 2022 Pertti Kinnia <pertti.kinnia@fmi.fi> 22.10.6-1.fmi
- Added support for IWXXM messages; BRAINSTORM-905

* Fri Jun 17 2022 Andris Pavēnis <andris.pavenis@fmi.fi> 22.6.17-1.fmi
- Add support for RHEL9. Update libpqxx to 7.7.0 (rhel8+) and fmt to 8.1.1

* Tue May 24 2022 Mika Heiskanen <mika.heiskanen@fmi.fi> - 22.5.24-1.fmi
- Repackaged due to NFmiArea ABI changes

* Tue Mar  8 2022 Mika Heiskanen <mika.heiskanen@fmi.fi> - 22.3.8-1.fmi
- Use the new TimeSeries library

* Thu Jan 27 2022 Pertti Kinnia <pertti.kinnia@fmi.fi> 22.1.27-1.fmi
- Using half open range when querying valid messages; BS-2245

* Tue Jan 25 2022 Pertti Kinnia <pertti.kinnia@fmi.fi> 22.1.25-1.fmi
- TAF's are stored e.g. every n'th (3rd) hour between xx:20 and xx:40 and then published. If configured, during publication hour delay latest messages until xx:40 for messages having 'messagevalidtime' time restriction. Take publication time into account with time range query too when querying valid messages (when validrangemessages=1); BRAINSTORM-2239

* Tue Dec  7 2021 Andris Pavēnis <andris.pavenis@fmi.fi> 21.12.7-1.fmi
- Update to postgresql 13 and gdal 3.3

* Tue Sep  7 2021 Andris Pavēnis <andris.pavenis@fmi.fi> 21.9.7-1.fmi
- Rebuild due to dependency changes

* Tue Aug 31 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.8.31-1.fmi
- Repackaged due to Spine ABI changes

* Tue Aug 17 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.8.17-1.fmi
- Repackaged due to Reactor ABI changes related to shutdowns

* Thu Jul  8 2021 Andris Pavēnis <andris.pavenis@fmi.fi> 21.7.8-1.fmi
- Use libpqxx7 for RHEL8

* Mon Jul  5 2021 Andris Pavēnis <andris.pavenis@fmi.fi> 21.7.5-1.fmi
- Use Fmi::Database::PostgreSQLConnection instead of local implementation

* Thu Jan 14 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.1.14-1.fmi
- Repackaged smartmet to resolve debuginfo issues

* Fri Jan  8 2021 Andris Pavenis <andris.pavenis@fmi.fi> - 21.1.8-1.fmi
- Build update (use makefile.inc, do not use cmake in director test)

* Tue Jan  5 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.1.5-1.fmi
- Upgraded libpqxx dependency

* Wed Sep 23 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.9.23-1.fmi
- Use Fmi::Exception instead of Spine::Exception
- New release version

* Tue Sep 22 2020 Pertti Kinnia <pertti.kinnia@fmi.fi> - 20.9.22-1.fmi
- Support for querying global scoped message types (e.g. SWX, TCA, VAA) for flight route (BS-1840)

* Fri Aug 21 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.8.21-1.fmi
- Upgrade to fmt 6.2

* Fri Jun 12 2020 Pertti Kinnia <pertti.kinnia@fmi.fi> - 20.6.12-1.fmi
- Configured message type scope (station, fir area or global) based queries (BS-1840)

* Mon Jun  8 2020 Pertti Kinnia <pertti.kinnia@fmi.fi> - 20.6.8-1.fmi
- Use FIR areas to query SIGMET, AIRMET, ARS and WXREP messages, and temporarily use FIR areas to query SWX, TCA and VAA messages too (BS-1833)
- Use message type restriction for record_set CTE to speed up queries (BS-1838)
- Upgraded libpqxx dependencies
- Use message type restriction for record_set CTE

* Sat Apr 18 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.4.18-1.fmi
- Upgraded to Boost 1.69

* Thu Mar 19 2020 Pertti Kinnia <pertti.kinnia@fmi.fi> - 20.3.19-1.fmi
- Added support for excluding finnish SPECIs (BS-1779)

* Mon Jan 20 2020 Andris Pavēnis <andris.pavenis@fmi.fi> - 20.1.20-1.fmi
- Return latest messages from all routes in case of distinct=0

* Fri May 10 2019 Pertti Kinnia <pertti.kinnia@fmi.fi> - 19.5.10-1.fmi
- Ignoring IWXXM messages (BRAINSTORM-1582)

* Mon Sep 24 2018 Pertti Kinnia <pertti.kinnia@fmi.fi> - 18.9.24-2.fmi
- Restored 'message_id' column name/alias for latest_messages CTE

* Mon Sep 24 2018 Pertti Kinnia <pertti.kinnia@fmi.fi> - 18.9.24-1.fmi
- Returning latest message by latest message time and creation time instead of max message_id (BS-1286)
- Escaping input literals (icao code, place (station name), country code etc) (BS-1352)
- Fixed messirheading grouping (type) for 'MessageValidTimeRangeLatest' message type (BS-1340)

* Wed Jul 25 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.7.25-1.fmi
- Prefer nullptr over NULL

* Sat Apr  7 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.4.7-1.fmi
- Upgrade to boost 1.66

* Tue Mar 20 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.3.20-1.fmi
- Full repackaging of the server

* Fri Feb  9 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.2.9-1.fmi
- Repackaged since base class SmartMetEngine size changed

* Mon Jan 15 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.1.15-1.fmi
- Recompiled with latest libpqxx

* Mon Aug 28 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.8.28-1.fmi
- Upgrade to boost 1.65

* Wed Mar 15 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.3.15-1.fmi
- Recompiled since Spine::Exception changed

* Tue Mar 14 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.3.14-1.fmi
- Switched to use macgyver StringConversion tools

* Wed Jan  4 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.1.4-1.fmi
- Changed to use renamed SmartMet base libraries

* Wed Nov 30 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.11.30-1.fmi
- Using test database in sample configuration
- No installation for configuration

* Tue Nov  1 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.11.1-1.fmi
- Namespace changed

* Tue Sep  6 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.9.6-1.fmi
- New exception handler

* Mon Aug 15 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.8.15-1.fmi
- Full recompile

* Tue Jun 14 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.6.14-1.fmi
- Full recompile

* Thu Jun  2 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.6.2-1.fmi
- Full recompile

* Wed Jun  1 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.6.1-1.fmi
- Added graceful shutdown

* Fri Mar  4 2016 Tuomo Lauri <tuomo.lauri@fmi.fi> - 16.3.4-1.fmi
- Applying metar filtering when queyrying messages created within given time range.

* Tue Feb  9 2016 Tuomo Lauri <tuomo.lauri@fmi.fi> - 16.2.9-1.fmi
- Rebuilt against the new TimeSeries::Value definition

* Tue Feb  2 2016 Tuomo Lauri <tuomo.lauri@fmi.fi> - 16.2.2-1.fmi
- Now using Timeseries::None - type

* Mon Jan 18 2016 Mika Heiskanen <mika.heiskanen@fmi.fi> - 16.1.18-1.fmi
- newbase API changed, full recompile

* Mon Dec 14 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.12.14-1.fmi
- Added query for messages created within given time range.
- Added configuration for SPECI and LOWWIND messages

* Thu Dec  3 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.12.3-1.fmi
- Various adjustments to behaviour

* Thu Nov 26 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.11.26-1.fmi
- TAF time handling corrected

* Wed Nov 25 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.11.25-1.fmi
- Now conforming to the current specifications

* Wed Nov 18 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.11.18-1.fmi
- Optimized queries
- Better handling of TAF-messages

* Mon Nov  9 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.11.9-1.fmi
- Using fast case conversion without locale locks when possible

* Mon Oct 26 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.10.26-1.fmi
- Added proper debuginfo packaging

* Mon Oct 12 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.10.12-1.fmi
- Ignored duplicate messages
- Adjusted message validity times

* Tue Sep 29 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.9.29-1.fmi
- Specification updates

* Wed Aug 26 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.8.26-1.fmi
- Fixed errorneous ORDER BY clause

* Thu Aug 20 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.8.20-1.fmi
- Bugfixes

* Tue Aug 18 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.8.18-1.fmi
- Recompile forced by brainstorm API changes

* Mon Aug 17 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.8.17-1.fmi
- Use -fno-omit-frame-pointer to improve perf use

* Fri Aug 14 2015 Mika Heiskanen <mika.heiskanen@fmi.fi> - 15.8.14-1.fmi
- Full recompile due to string formatting changes

* Wed Aug 12 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.8.12-1.fmi
- Removed debug outputting and configuration

* Tue Aug  4 2015 Tuomo Lauri <tuomo.lauri@fmi.fi> - 15.8.4-1.fmi
  - Initial release of aviation message engine
