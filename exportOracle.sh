#!/bin/bash
jdbc_url=jdbc:oracle:thin:@localhost:1521:XE
jdbc_user=int_tests
jdbc_password=int_tests
tool_builddir=target/exportOracle
JDBC_CLASSPATH=lib/ojdbc7_g.jar:lib/orai18n.jar

. export.sh $1
