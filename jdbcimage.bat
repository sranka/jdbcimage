@setlocal
@echo off
set JAVA_OPTS=-Xmx1024m
java %JAVA_OPTS% -classpath "%~dp0jdbcimage.jar;%~dp0lib/*" io.github.sranka.jdbcimage.main.JdbcImageMain %*
