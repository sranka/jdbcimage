#!/bin/bash
jdbc_url=jdbc:postgresql://localhost:5432/inttests?reWriteBatchedInserts=true\&currentSchema=pavel
jdbc_user=int_tests
jdbc_password=int_tests
tool_builddir=target/exportPostgresql
JDBC_CLASSPATH=lib/postgresql-9.4.jar

. import.sh $1
