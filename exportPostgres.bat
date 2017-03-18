@setlocal
@echo off
set jdbc_url=jdbc:postgresql://localhost:5432/em?currentSchema=em
set jdbc_user=hpem
set jdbc_password=changeit
set tool_builddir=target\exportPostgresql
set JDBC_CLASSPATH=lib/postgresql-9.4.jar

call export.bat %1