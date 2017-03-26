@setlocal
@echo off
set JAVA_OPTS=-Xmx1024m
java %JAVA_OPTS% -classpath ""%~dp0../target/jdbc-image-tool.jar;%~dp0../lib/*" pz.tool.jdbcimage.main.JdbcImageMain %*
