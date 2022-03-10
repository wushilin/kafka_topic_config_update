#!/bin/sh
CP=.
for i in `find build -name '*.jar' 2>/dev/null`
do
  CP=$CP:$i
done

for i in `ls -1 *.jar 2>/dev/null`
do
  CP=$CP:$i
done
java -classpath $CP $@
