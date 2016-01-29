@setlocal
@echo off
set tool_in_file=%1%
set tool_out_file=%tool_in_file%.dump

set OPTS=-Xmx512m
set OPTS=%OPTS% "-Dtool_in_file=%tool_in_file%"
set OPTS=%OPTS% "-Dtool_out_file=%tool_out_file%"

mkdir %tool_builddir%
java %OPTS% -classpath target/jdbc-image-tools.jar;lib/ojdbc7_g.jar;lib/orai18n.jar pz.tool.jdbcimage.main.TableFileDump
