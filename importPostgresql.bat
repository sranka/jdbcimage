@setlocal
@echo off
set jdbc_url=jdbc:postgresql://localhost:5432/XE
set jdbc_user=hpem
set jdbc_password=changeit
set tool_builddir=target\exportPostgresql
set tool_parallelism=7

set OPTS=-Xmx256m
set OPTS=%OPTS% "-Djdbc_url=%jdbc_url%"
set OPTS=%OPTS% "-Djdbc_user=%jdbc_user%"
set OPTS=%OPTS% "-Djdbc_password=%jdbc_password%"
set OPTS=%OPTS% "-Dtool_builddir=%tool_builddir%"
set OPTS=%OPTS% "-Dtool_parallelism=%tool_parallelism%"
set OPTS=%OPTS% "-Dtool_disableIndexes=%tool_disableIndexes%"
set OPTS=%OPTS% "-Dtool_waitOnStartup=false"

java %OPTS% -classpath target/jdbc-image-tools.jar;lib/postgresql-9.4.jar pz.tool.jdbcimage.main.MultiTableParallelImport %1
