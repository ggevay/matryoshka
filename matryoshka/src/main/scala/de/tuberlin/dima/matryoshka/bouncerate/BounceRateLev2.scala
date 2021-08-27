package de.tuberlin.dima.matryoshka.bouncerate

import de.tuberlin.dima.matryoshka.util.Util._
import Util._
import de.tuberlin.dima.matryoshka.util.RDDSplitting._
import org.apache.spark.SparkContext

object BounceRateLev2 {

  def main(args: Array[String]): Unit = {

    val skewed = args.length == 3

    val totalSize = args(0).toLong
    val numGroups = args(1).toInt
    val exponent =
      if (skewed) {
        args(2).toDouble
      } else {
        0
      }

    implicit val sc: SparkContext = sparkSetup(this)

    val visitsAll =
      if (skewed) {
        getBounceRateSkewedRandomInput(totalSize, numGroups, exponent)
      } else {
        getBounceRateRandomInput(totalSize, numGroups)
      }

    val stopWatch = new StopWatch

    val grouped = visitsAll.defaultPersist().groupByKeyIntoRDDs()

    val res = grouped.map {case (gr, visits) =>
      visits.defaultPersist()
      val countsPerIP = visits.map((_, 1)).reduceByKey(_+_)
      val numBounces = countsPerIP.filter(_._2 == 1).count()
      val numTotalVisitors = visits.distinct().count()
      numBounces.toDouble / numTotalVisitors.toDouble
    }

    res.print()
    //res.sorted.print()

    stopWatch.done()
  }
}
