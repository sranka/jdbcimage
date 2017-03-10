#!/bin/bash
tool_in_file=$1
tool_out_file=$2

OPTS=-Xmx256m
OPTS="$OPTS -Dtool_in_file=$tool_in_file"
OPTS="$OPTS -Dtool_out_file=$tool_out_file"
OPTS=$OPTS "-Dtool_skip_data=true"

java $OPTS -classpath target/jdbc-image-tools.jar pz.tool.jdbcimage.main.TableFileDump

