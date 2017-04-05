@setlocal
@echo off
set JAVA_OPTS=-Xmx1024m
java %JAVA_OPTS% -classpath ""%~dp0target/jdbcimage.jar;%~dp0lib/*" pz.tool.jdbcimage.main.JdbcImageMain %*
