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

exec java -Xmx30g -verbosegc -Xloggc:/var/log/metron/gc.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintClassHistogram -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=100 -XX:GCLogFileSize=2M -XX:+HeapDumpOnOutOfMemoryError -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:G1ReservePercent=10 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 -XX:G1HeapRegionSize=8m -XX:InitiatingHeapOccupancyPercent=70 -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=10102 -agentpath:/opt/yjp/bin/linux-x86-64/libyjpagent.so=disablestacktelemetry,exceptions=disable,delay=10000,usedmem=69,logdir=/var/log/data -Dlogback.configurationFile=logback.xml -jar opentsdb-2.3.0-fat.jar --config=${STAGE_CONFIG_FILE}
