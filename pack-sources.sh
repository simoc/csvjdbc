#!/bin/bash

JAR=../LizardConverter/dist/csvjdbc.jar
SOURCES=$(find src/java/org/relique/ -iname "*.java")
TESTSOURCES=src/java/test/org/relique/jdbc/csv/*.java
TESTDATA=$(find src/testdata -maxdepth 1 -type f -not -name "*~")

tar czf dist/csvjdbc-sources.tar.gz $SOURCES
tar czf dist/csvjdbc-sources-full.tar.gz $SOURCES $TESTSOURCES $TESTDATA
cp $JAR dist/.
