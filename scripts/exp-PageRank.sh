#!/bin/bash

# "Strict mode", see http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail


run-exp () {
  local classname=$1
  local days=$2
  local numVertices=$3
  local numEdges=$4
  local LBLSJoinStrategy=$5
  ./exp-spark5.sh $resultDir/result-$classname-$LBLSJoinStrategy-$days-$numVertices-$numEdges.csv conf/nested/conf_PageRankSpark de.tuberlin.dima.matryoshka.pagerank.$classname TOBEFILLED.jar $epsilon $days $numVertices $numEdges $LBLSJoinStrategy
}


epsilon=$1
totalSizeExp=$2
edgesVerticesRatio=$3
resultDirPrefix=$4

totalSize=$[2**$totalSizeExp]
numEdges=$totalSize
resultDir=$resultDirPrefix/totalSize_${totalSize}_${totalSizeExp}
mkdir -p $resultDir

#for i in 2 4 8 16 32 64 128
#for i in 64 128 256 512 1024
#for i in 1 2 4 8 16 32 64 128 256 512
for i in TOBEFILLED
#for i in 32
#for i in 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576
#for i in 32768 65536 131072 262144 524288 1048576
#for i in 2097152 4194304
#for i in 4194304 2097152
#for i in 1048576
#for i in 33554432 1 1024 16777216 8388608 4194304 2097152 1048576
#for i in 128 256 512 2048 4096 8192 16384 32768 65536 131072 262144 524288
#for i in 33554432 1 1024 16777216 8388608 4194304 2097152 1048576 128 256 512 2048 4096 8192 16384 32768 65536 131072 262144 524288
#for i in 32768 65536 131072 262144 524288
#for i in 1024 2048
#for i in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304 8388608 16777216 33554432
#for i in 8192 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304 8388608 16777216 33554432
#for i in 2 4 8 16 32 64 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304 8388608 16777216 33554432
do

  days=$i
  numVertices=$[$numEdges/$edgesVerticesRatio/$days]

  #run-exp PageRankLev12 $days $numVertices $numEdges Broadcast
  #run-exp PageRankLev12 $days $numVertices $numEdges Repartition
  run-exp PageRankLev12 $days $numVertices $numEdges Optimizer

  #if [ $i -lt 129 ]; then
    run-exp PageRankLev2 $days $numVertices $numEdges -
  #fi

  #if [ $i -gt 7 ]; then
    run-exp PageRankLev1 $days $numVertices $numEdges -
  #fi

done

