#!/bin/bash
sh build.sh pom.xml
mvn -X -Dmaven.test.skip=true -Dmaven.javadoc.skip=true package
