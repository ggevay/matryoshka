#!/bin/bash

#for i in {11..37}
#do
#  ssh cloud-$i "ps aux |grep java |grep -v datanode |grep -v namenode |grep -v grep |grep -v check-java-processes |grep -v nailgun" &
#done
#wait

pdsh -w TOBEFILLED 'ps aux |grep java |grep -v datanode |grep -v namenode |grep -v grep |grep -v check-java-processes |grep -v nailgun' 2>&1 |grep -v 'ssh exited with exit code 1'

pdsh -w TOBEFILLED 'ps aux |grep mariadb |grep -v grep |grep -v ProcMon' 2>&1 |grep -v 'ssh exited with exit code 1'
