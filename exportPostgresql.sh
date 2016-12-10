#!/bin/bash
jdbc_url=jdbc:postgresql://localhost:5432/em?currentSchema=em
jdbc_user=hpem
jdbc_password=changeit
tool_builddir=target/exportPostgresql
JDBC_CLASSPATH=lib/postgresql-9.4.jar

. export.sh $1