#!/bin/bash

JAR=../LizardConverter/dist/csvjdbc.jar
find src/main/java/org/relique/ -iname "*.java" > sources.txt
cat sources.txt > sources-full.txt
/bin/ls src/java/test/org/relique/jdbc/csv/*.java >> sources-full.txt
find src/testdata -maxdepth 1 -type f -not -name "*~" >> sources-full.txt

tar czf dist/csvjdbc-sources.tar.gz -T sources.txt
tar czf dist/csvjdbc-sources-full.tar.gz -T sources-full.txt
cp $JAR dist/.
/bin/rm sources.txt sources-full.txt

