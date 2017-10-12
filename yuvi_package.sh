#!/usr/bin/bash
sh build.sh pom.xml
mvn test-compile
mvn -X -Dmaven.test.skip=true -Dmaven.javadoc.skip=true package
