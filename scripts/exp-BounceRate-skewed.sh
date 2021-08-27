#!/bin/bash

run-exp () {
  local classname=$1
  local totalSize=$2
  local numGroups=$3
  local skewExponent=$4
  ./exp-spark5.sh $resultDir/result-$classname-$totalSize-$numGroups.csv conf/nested/conf_BounceRateSpark de.tuberlin.dima.matryoshka.bouncerate.$classname TOBEFILLED.jar $totalSize $numGroups $skewExponent
}


totalSizeExp=$1
skewExponent=$2
resultDirPrefix=$3

totalSize=$[2**$totalSizeExp]
resultDir=$resultDirPrefix/totalSize_${totalSize}_skew_${skewExponent}
mkdir -p $resultDir

#for i in 2 4 8 16 32 64 128
#for i in 1 2 4 8 16 32 64 128 256 512
#for i in 128 256 512
#for i in 64 128
for i in TOBEFILLED
do
  numGroups=$i

  run-exp BounceRateLev12 $totalSize $numGroups $skewExponent

  #if [ $i -lt 65 ]; then
    run-exp BounceRateLev2 $totalSize $numGroups $skewExponent
  #fi

  #if [ $i -gt 3 ]; then
    run-exp BounceRateLev1 $totalSize $numGroups $skewExponent
  #fi

done

