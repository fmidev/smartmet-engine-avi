%define DIRNAME avi
%define LIBNAME smartmet-%{DIRNAME}
%define SPECNAME smartmet-engine-%{DIRNAME}
Summary: SmartMet aviation message engine
Name: %{SPECNAME}
Version: 20.3.19
Release: 1%{?dist}.fmi
License: FMI
Group: SmartMet/Engines
URL: https://github.com/fmidev/smartmet-engine-avi
Source0: %{name}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildRequires: rpm-build
BuildRequires: gcc-c++
BuildRequires: make
BuildRequires: boost-devel
BuildRequires: libconfig >= 1.4.9
BuildRequires: libpqxx-devel
BuildRequires: zlib-devel
BuildRequires: bzip2-devel
BuildRequires: smartmet-library-spine-devel >= 20.3.9
BuildRequires: smartmet-library-macgyver >= 20.3.5
Requires: boost-date-time
Requires: smartmet-library-macgyver >= 20.3.5
Requires: libconfig >= 1.4.9
Requires: smartmet-library-spine >= 20.3.9
Provides: %{SPECNAME}
Obsoletes: smartmet-brainstorm-aviengine < 16.11.1
Obsoletes: smartmet-brainstorm-aviengine-debuginfo < 16.11.1

%description
SmartMet aviation message engine

%package -n %{SPECNAME}-devel
Summary: SmartMet %{SPECNAME} development headers
Group: SmartMet/Development
Provides: %{SPECNAME}-devel
Requires: libpqxx-devel
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
