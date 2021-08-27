#!/bin/bash

# "Strict mode", see http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail


run-exp () {
  local classname=$1
  local numComp=$2
  local numVert=$3
  local numEdges=$4
  local sampleSize=$5
  ./exp-spark5.sh $resultDir/result-$classname-$numComp-$numVert-$numEdges-$sampleSize.csv conf/nested/conf_AvgDistancesSpark de.tuberlin.dima.matryoshka.avgdistances.$classname TOBEFILLED.jar $numComp $numVert $numEdges $sampleSize
}


numVertExp=$1
numEdgesExp=$2
sampleSizeExp=$3
resultDirPrefix=$4

numVert=$[2**$numVertExp]
numEdges=$[2**$numEdgesExp]
sampleSize=$[2**$sampleSizeExp]
resultDir=$resultDirPrefix/numVertExp_${numVertExp}_numEdgesExp_${numEdgesExp}_sampleSizeExp_${sampleSizeExp}
mkdir -p $resultDir

#for i in 1 2 4 8 16 32
#for i in 128 256 512 1024
#for i in 131072
for i in TOBEFILLED
do

  numComp=$i

  run-exp AvgDistancesLev123 $numComp $numVert $numEdges $sampleSize

  if [ $i -gt 0 ]; then
    run-exp AvgDistancesLev1 $numComp $numVert $numEdges $sampleSize
  fi

#  if [ $i -gt 0 ]; then
#    run-exp AvgDistancesLev12 $numComp $numVert $numEdges $sampleSize
#  fi

#  if [ $i -lt 65 ]; then
#    run-exp AvgDistancesLev23 $numComp $numVert $numEdges $sampleSize
#  fi

  if [ $i -lt 33 ]; then
    run-exp AvgDistancesLev3 $numComp $numVert $numEdges $sampleSize
  fi

done

