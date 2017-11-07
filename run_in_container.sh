#!/bin/bash
# Start pinshot process inside a docker container with java installed
# Some settings can be overriden by environment variables as shown below

ulimit -n 65536

export SERVICENAME=${SERVICENAME:=opentsdb-metron}
export JAVA_MAIN=${JAVA_MAIN:=net.opentsdb.tools.TSDMain}
export LOGBACK_FILE=${LOGBACK_FILE:=logback.xml}
HEAP_SIZE=${HEAP_SIZE:=512m}
NEW_SIZE=${NEW_SIZE:=256m}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PARENT_DIR="$(dirname $DIR)"

LOG_DIR=/var/log/${SERVICENAME}
CP=${PARENT_DIR}/*jar

exec java -Dlogback.configurationFile=logback.xml -jar opentsdb-2.3.0-fat.jar --config=yuvi.conf
