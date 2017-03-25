#!/bin/bash

# TODO heapsize as an argument, 256m being the default
OPTS=-Xmx1024m
java $JAVA_OPTS $OPTS -classpath "`dirname $0`/../target/jdbc-image-tool.jar:`dirname $0`/../lib/*" pz.tool.jdbcimage.main.JdbcImageMain $*
