#!/bin/bash
jdbc_url="jdbc:mysql://localhost:3306/pricefx_qa?characterEncoding=utf8&rewriteBatchedStatements=true"
jdbc_user=root
jdbc_password=root
tool_builddir=target/exportMariadb
JDBC_CLASSPATH=lib/mariadb-java-client-1.5.8.jar

. export.sh $1