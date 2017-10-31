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

exec java -server -Xmx${HEAP_SIZE} -Xms${HEAP_SIZE} -XX:NewSize=${NEW_SIZE} -XX:MaxNewSize=${NEW_SIZE} \
    -verbosegc -Xloggc:${LOG_DIR}/gc.log \
    -XX:+UnlockDiagnosticVMOptions -XX:ParGCCardsPerStrideChunk=4096 -XX:+PerfDisableSharedMem \
    -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=100 -XX:GCLogFileSize=2M \
    -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintClassHistogram \
    -XX:+HeapDumpOnOutOfMemoryError -XX:+UseParNewGC \
    -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=60 -XX:+UseCMSInitiatingOccupancyOnly \
    -XX:ErrorFile=${LOG_DIR}/jvm_error.log -Dnetworkaddress.cache.ttl=60 -Djava.net.preferIPv4Stack=true \
    -cp ${CP} -Dlogback.configurationFile=${LOGBACK_FILE} -Dstage_config=${STAGE_CONFIG_FILE} \
    -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=10102 \
    -Dfile.encoding=UTF-8\
    ${JAVA_MAIN} --config=yuvi.conf
