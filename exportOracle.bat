@setlocal
@echo off
set jdbc_url=jdbc:oracle:thin:@localhost:1521:XE
set jdbc_user=hpem
set jdbc_password=changeit
set tool_builddir=target\exportOracle
set JDBC_CLASSPATH=lib/ojdbc7_g.jar;lib/orai18n.jar

call export.bat %1