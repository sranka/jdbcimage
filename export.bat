@setlocal
@echo off
set OPTS=-Xmx256m
set OPTS=%OPTS% "-Djdbc_url=%jdbc_url%"
set OPTS=%OPTS% "-Djdbc_user=%jdbc_user%"
set OPTS=%OPTS% "-Djdbc_password=%jdbc_password%"
set OPTS=%OPTS% "-Dtool_builddir=%tool_builddir%"
rem set OPTS=%OPTS% "-Dtool_concurrency=7"
set OPTS=%OPTS% "-Dtool_ignoreEmptyTables=false"
set OPTS=%OPTS% "-Dtool_waitOnStartup=false"

IF [%1] == [] mkdir %tool_builddir%
java %OPTS% -classpath target/jdbc-image-tools.jar;%JDBC_CLASSPATH% pz.tool.jdbcimage.main.MultiTableParallelExport %1
