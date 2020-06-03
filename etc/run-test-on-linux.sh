#!/bin/sh
##### PG TEST #####
export JAVA_HOME=/home/lin/apps/jdk14
export PATH=$JAVA_HOME/bin:$PATH
java -Dfile.encoding=UTF8 -jar tools-postgres.jar -h="172.29.40.41" -p=5432 -l=postgres -w=1 -k=demo -s=200 -n=10 -c=15000 -r=true -m=MULTI