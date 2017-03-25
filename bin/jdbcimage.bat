@setlocal
@echo off
# TODO heapsize as an argument, 256m being the default
set OPTS=-Xmx1024m
java %OPTS% -classpath ""%~dp0../target/jdbc-image-tool.jar;%~dp0../lib/*" pz.tool.jdbcimage.main.MultiTableParallelExport %1
