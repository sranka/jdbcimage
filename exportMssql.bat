@setlocal
@echo off
set jdbc_url=jdbc:sqlserver://localhost:1433;databaseName=XE
set jdbc_user=hpem
set jdbc_password=changeit
set tool_builddir=target\exportMssql
set JDBC_CLASSPATH=lib/sqljdbc4.jar

call export.bat %1

