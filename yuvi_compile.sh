#!/bin/sh
sh build.sh pom.xml
cp pom-yuvi.xml pom.xml
mvn compile
cp target/generated-sources/net/opentsdb/tools/BuildData.java src-main/net/opentsdb/tools
cp -r target/generated-sources/net/opentsdb/query/expression/parser src-main/net/opentsdb/query/expression
cp opentsdb.conf.yuvi opentsdb.conf
rm src-main/net/opentsdb/tsd/client
