#!/bin/bash

# pass any -X and -D option to java
while [[ $1 == "-X"* ]] || [[ $1 == "-D"* ]] ; do
  JAVA_OPTS="${JAVA_OPTS} $1"
  shift
done
if [[ $JAVA_OPTS != *"-Xmx"* ]] ; then
   if [[ $1 == "import" ]] ; then
     JAVA_OPTS="$JAVA_OPTS -Xmx1024m"
   else
     JAVA_OPTS="$JAVA_OPTS -Xmx256m"
   fi
fi

echo "Starting Java with:$JAVA_OPTS"
java $JAVA_OPTS $OPTS -classpath "`dirname $0`/../target/jdbc-image-tool.jar:`dirname $0`/../lib/*" pz.tool.jdbcimage.main.JdbcImageMain $*
