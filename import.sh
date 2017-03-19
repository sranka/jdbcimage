#!/bin/bash

if [[ "$heap_size" == "" ]]; then
  OPTS=-Xmx256m
else
  OPTS=-Xmx$heap_size
fi
OPTS="$OPTS -Djdbc_url=$jdbc_url"
OPTS="$OPTS -Djdbc_user=$jdbc_user"
OPTS="$OPTS -Djdbc_password=$jdbc_password"
OPTS="$OPTS -Dtool_builddir=$tool_builddir"
OPTS="$OPTS -Dtool_concurrency=$tool_concurrency"
OPTS="$OPTS -Dtool_ignoreEmptyTables=false"
OPTS="$OPTS -Dtool_waitOnStartup=false"
OPTS="$OPTS -Dbatch.size=100"
OPTS="$OPTS -Dignored_tables=schemaversion"

if [ -z "$1" ] ; then mkdir -p $tool_builddir ; fi
java $OPTS -classpath "target/jdbc-image-tools.jar:$JDBC_CLASSPATH:lib/*" pz.tool.jdbcimage.main.MultiTableParallelImport $*
