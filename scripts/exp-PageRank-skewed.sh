#!/bin/bash

# "Strict mode", see http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail


run-exp () {
  local classname=$1
  local days=$2
  local numVertices=$3
  local numEdges=$4
  local skewExponent=$5
  ./exp-spark5.sh $resultDir/result-$classname-$days-$numVertices-$numEdges.csv conf/nested/conf_PageRankSpark de.tuberlin.dima.matryoshka.pagerank.$classname TOBEFILLED.jar $epsilon $days $numVertices $numEdges $skewExponent
}


epsilon=$1
totalSizeExp=$2
skewExponent=$3
resultDirPrefix=$4

totalSize=$[2**$totalSizeExp]
numEdges=$totalSize
resultDir=$resultDirPrefix/totalSize_${totalSize}_${totalSizeExp}_skew_${skewExponent}
mkdir -p $resultDir

#for i in 2 4 8 16 32 64 128
#for i in 64 128 256 512 1024
#for i in 1 2 4 8 16 32 64 128 256 512 1024
#for i in 64 128 256 512
for i in TOBEFILLED
do

  days=$i
  numVertices=$[$numEdges/128/$days]

  run-exp PageRankLev12 $days $numVertices $numEdges $skewExponent

  #if [ $i -gt 7 ]; then
    run-exp PageRankLev1 $days $numVertices $numEdges $skewExponent
  #fi

  #if [ $i -lt 33 ]; then
    run-exp PageRankLev2 $days $numVertices $numEdges $skewExponent
  #fi

done

