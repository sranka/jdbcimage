#!/bin/bash
jdbc_url=jdbc:sqlserver://localhost:1433;databaseName=XE
jdbc_user=hpem
jdbc_password=changeit
tool_builddir=target/exportMssql
JDBC_CLASSPATH=lib/sqljdbc4.jar

. import.sh $1

