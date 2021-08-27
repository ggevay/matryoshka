#!/bin/bash

i=0

while [ $(./quick-check-java-processes.sh | wc -l) -ne 0 ]
do
    echo "There are java processes running. Waiting..."
    i=$[i+1]
    #if [ $i -gt 3 ]; then
    #    echo "Timeout. Killing all java except HDFS."
    #    ./kill-all-java-except-hdfs.sh
    #fi
done
