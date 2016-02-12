@setlocal
@echo off
set jdbc_url=jdbc:sqlserver://localhost:1433;databaseName=XE
set jdbc_user=hpem
set jdbc_password=changeit
set tool_builddir=target\exportMssql
set tool_parallelism=7
set tool_ignoreEmptyTables=false

set OPTS=-Xmx256m
set OPTS=%OPTS% "-Djdbc_url=%jdbc_url%"
set OPTS=%OPTS% "-Djdbc_user=%jdbc_user%"
set OPTS=%OPTS% "-Djdbc_password=%jdbc_password%"
set OPTS=%OPTS% "-Dtool_builddir=%tool_builddir%"
set OPTS=%OPTS% "-Dtool_parallelism=%tool_parallelism%"
set OPTS=%OPTS% "-Dtool_ignoreEmptyTables=%tool_ignoreEmptyTables%"
set OPTS=%OPTS% "-Dtool_waitOnStartup=false"

mkdir %tool_builddir%
java %OPTS% -classpath target/jdbc-image-tools.jar;lib/sqljdbc4.jar pz.tool.jdbcimage.main.MultiTableParallelExport
