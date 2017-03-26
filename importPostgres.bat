@setlocal
@echo off
set jdbc_url=jdbc:postgresql://localhost:5432/em?currentSchema=em
set jdbc_user=hpem
set jdbc_password=changeit
set tool_builddir=target\exportPostgresql
set tool_concurrency=7

set OPTS=-Xmx256m
set OPTS=%OPTS% "-Djdbc_url=%jdbc_url%"
set OPTS=%OPTS% "-Djdbc_user=%jdbc_user%"
set OPTS=%OPTS% "-Djdbc_password=%jdbc_password%"
set OPTS=%OPTS% "-Dtool_builddir=%tool_builddir%"
set OPTS=%OPTS% "-Dtool_concurrency=%tool_concurrency%"
set OPTS=%OPTS% "-Dtool_disableIndexes=%tool_disableIndexes%"
set OPTS=%OPTS% "-Dtool_waitOnStartup=false"

java %OPTS% -classpath target/jdbc-image-tool.jar;lib/postgresql-9.4.jar pz.tool.jdbcimage.main.MultiTableParallelImport %1
