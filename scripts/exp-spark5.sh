#!/bin/bash


# Strict mode, see http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail


if [ "$#" -le 3 ]; then
    echo "Illegal number of parameters"
    exit 3
fi


echo "Script arguments: $@"
resFile=$1
config=$2
class=$3
jar=$4
shift 4 # https://stackoverflow.com/questions/1537673/how-do-i-forward-parameters-to-other-command-in-bash-script

if [ -z ${SPARK_CONF_EXTRA+x} ]; 
then
    SPARK_CONF_EXTRA=''
fi

logDir=TOBEFILLED

sparkPath=TOBEFILLED # warning: this script deletes $sparkPath/logs/* and $sparkPath/work/*


# Create result file, and check that it had not already existed.
if [ -f $resFile ]; then
  echo "!!! $resFile file exists. Exiting."
  exit 73
  #mv $resFile $resFile.old
  #echo "Moved old resFile"
fi
mkdir -p `dirname $resFile`
touch $resFile


# Stop Spark cluster on script exit in any case.
function finish {
  set +e
  printf "\nScript exiting. Stopping the Spark cluster.\n"
  $sparkPath/sbin/stop-all.sh >/dev/null
  wait
}
trap finish EXIT


# Copy configuration.
set +e
while :
do
  cp $config/* $sparkPath/conf
  if [ $? -eq 0 ]; then
    break
  else
    echo "Trying again"
  fi
done
set -e



mkdir -p $logDir


if [ $(./quick-check-java-processes.sh | wc -l) -ne 0 ]; then
    echo "!!! There are java processes running. Exiting !!!"
    exit 80
fi



#for numMachines in {24..2..-2}; do
for numMachines in TOBEFILLED; do
#for numMachines in 2 4 6 8 12 16 20 24; do
  echo "numMachines: $numMachines"

  echo -n "$numMachines $class $@ ">>$resFile

  ./create-conf-para-spark.sh $sparkPath $numMachines 48 $config/spark-defaults.conf.paratemplate

  for ((rep = 1 ; rep <= 3 ; rep++)); do

      fail=false

      rm -rf $sparkPath/logs/* 
      rm -rf $sparkPath/work/*

      echo "Starting cluster for repeat $rep."
      $sparkPath/sbin/start-all.sh >$(mktemp $logDir/start-cluster.XXXXXX)
      
      #sleep 3

      if [ "$fail" = false ]; then
          tmpOut=$(mktemp $logDir/out.XXXXXXXX)

          echo -n "Running program ($tmpOut)"

          set +e
          $sparkPath/bin/spark-submit --master spark://TOBEFILLED:7077 $SPARK_CONF_EXTRA --class $class $jar "$@" &>$tmpOut
          if [ $? != 0 ]; then
            fail=true
          fi
          ./check-spark-success.sh $tmpOut
          if [ $? != 0 ] || [ $fail = true ]; then
            echo "!!!!!!!!!!!!! Spark program failed (out file: $tmpOut). Moving on."
            fail=true 
            set -e
          fi
          set -e

          if [ "$fail" = false ]; then
            # Get the execution time from the output
            export rtmarker="non-warmup-time: "
            ./parse-time.sh $tmpOut >>$resFile
            echo -n " " >>$resFile
          else
            echo -n "fail($tmpOut) " >>$resFile
          fi
          echo ". "
          gzip $tmpOut
      fi
        
      echo "Shutting down Spark cluster."
      $sparkPath/sbin/stop-all.sh >$(mktemp $logDir/stop-cluster.XXXXXXX)
      ./loop-until-no-j.sh # Loop until the cluster has really stopped

  done

  echo >>$resFile  #newline
  echo

done
