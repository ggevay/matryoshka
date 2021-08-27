#!/bin/bash

# Creates flink-conf for the specified number of machines.

sparkPath=$1
numMachines=$2
tasksPerMachine=$3
template=$4 #template for spark-defaults.conf; should contain the string 'gggpara' at the place of the parallelism

slavesTemplate=TOBEFILLED  # a file with the names of all machines of the cluster

if [ `cat $slavesTemplate |wc -l` -lt $numMachines ]; then
  echo "numMachines: $numMachines is larger then number of machines in slaves template"
  exit 1
fi


# There is this "Resource temporarily unavailable" problem, so retry (see https://stackoverflow.com/questions/5274294/how-can-you-run-a-command-in-bash-over-until-success)
# (This could be improved like this: https://stackoverflow.com/questions/5195607/checking-bash-exit-status-of-several-commands-efficiently)
while :
do
  sed s/gggpara/$(( $numMachines*$tasksPerMachine ))/ $template >$sparkPath/conf/spark-defaults.conf
  if [ $? -eq 0 ]; then
    break
  else
    echo "Trying again"
  fi
done

while :
do 
  head -n $numMachines $slavesTemplate >$sparkPath/conf/slaves
  if [ $? -eq 0 ]; then
    break
  else
    echo "Trying again"
  fi
done


