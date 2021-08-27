#!/bin/bash

run-exp () {
  local classname=$1
  local totalSize=$2
  local numGroups=$3
  ./exp-spark5.sh $resultDir/result-$classname-$totalSize-$numGroups.csv conf/nested/conf_BounceRateSpark de.tuberlin.dima.matryoshka.bouncerate.$classname TOBEFILLED.jar $totalSize $numGroups
}


totalSizeExp=$1
resultDirPrefix=$2
totalSize=$[2**$totalSizeExp]
resultDir=$resultDirPrefix/totalSize_$totalSize
mkdir -p $resultDir

#for i in 2 4 8 16 32 64 128
#for i in 1 2 4 8 16 32 64 128 256 512
for i in TOBEFILLED
#for i in 128 256 512
#for i in 64 128
#for i in 256
do
  numGroups=$i

  run-exp BounceRateLev12 $totalSize $numGroups

  #if [ $i -lt 65 ]; then
    run-exp BounceRateLev2 $totalSize $numGroups
  #fi

  if [ $i -gt 31 ]; then
    run-exp BounceRateLev1 $totalSize $numGroups
  fi

done

