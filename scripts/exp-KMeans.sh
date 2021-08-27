#!/bin/bash

run-exp () {
  local classname=$1
  local numPoints=$2
  local numRuns=$3
  local withClosureStrategy=$4
  local autoCoalesce=$5
  export SPARK_CONF_EXTRA="--conf spark.nesting.autoCoalesce=$autoCoalesce"
  ./exp-spark5.sh $resultDir/result-$classname-$withClosureStrategy-ac$autoCoalesce-$numPoints-$K-2-$numRuns.csv conf/nested/conf_KMeansSpark de.tuberlin.dima.matryoshka.kmeans.$classname TOBEFILLED.jar $numPoints $K 2 $numRuns $withClosureStrategy
}


totalSizeExp=$1
K=$2
resultDirPrefix=$3
totalSize=$[2**$totalSizeExp]
resultDir=$resultDirPrefix/totalSize_$totalSize
mkdir -p $resultDir

#for i in 1 2 4 8 16
#for i in 1 2 4 8 16 32 64 128 256 512
#for i in 512 256 128 64 32 16 8 4 2 1
#for i in 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576
#for i in 1048576 524288 262144 131072 65536 32768 16384 8192 4096 2048 1024
#for i in 8192
#for i in 512 1024 2048
#for i in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576
#for i in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536
#for i in 16384 32768 65536 131072
for i in TOBEFILLED
do

  numPoints=$[$totalSize / $i]
  numRuns=$i
  
  #run-exp KMeansLev12 $numPoints $numRuns Optimizer false # (false refers to autoCoalesce here)
  run-exp KMeansLev12 $numPoints $numRuns Optimizer true

  #run-exp KMeansLev12 $numPoints $numRuns BroadcastLeft true
  #run-exp KMeansLev12 $numPoints $numRuns BroadcastRight true
  #run-exp KMeansLev12 $numPoints $numRuns CartesianRDD true

  if [ $i -lt 129 ]; then
    run-exp KMeansLev2 $numPoints $numRuns - true
  fi

  if [ $i -gt 31 ]; then
    run-exp KMeansLev1 $numPoints $numRuns - true
  fi

done


