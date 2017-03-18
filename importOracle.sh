#!/bin/bash
heap_size=1024m
jdbc_url=jdbc:oracle:thin:@localhost:1521:XE
jdbc_user=int_tests
jdbc_password=int_tests
tool_builddir=target/importOracle
JDBC_CLASSPATH=lib/ojdbc7_g.jar:lib/orai18n.jar

. import.sh $1
