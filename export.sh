#!/bin/bash
OPTS=-Xmx256m
OPTS="$OPTS -Djdbc_url=$jdbc_url"
OPTS="$OPTS -Djdbc_user=$jdbc_user"
OPTS="$OPTS -Djdbc_password=$jdbc_password"
OPTS="$OPTS -Dtool_builddir=$tool_builddir"
# OPTS="$OPTS -Dtool_concurrency=7"
OPTS="$OPTS -Dtool_ignoreEmptyTables=false"
OPTS="$OPTS -Dtool_waitOnStartup=false"

if [ -z "$1" ] ; then mkdir -p $tool_builddir ; fi
java $OPTS -classpath "target/jdbc-image-tools.jar:$JDBC_CLASSPATH" pz.tool.jdbcimage.main.MultiTableParallelExport $1
