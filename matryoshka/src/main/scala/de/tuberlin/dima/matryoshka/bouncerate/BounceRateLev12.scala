package de.tuberlin.dima.matryoshka.bouncerate

import Util._
import de.tuberlin.dima.matryoshka.lifting._
import de.tuberlin.dima.matryoshka.util.Util._
import org.apache.spark.SparkContext

object BounceRateLev12 {

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

//    // Group sizes:
//    //println(visitsAll.map{case (k,_) => (k,1L)}.reduceByKey(_+_).map(_._2).max())
//    visitsAll.map{case (k,_) => (k,1L)}.reduceByKey(_+_).map(_._2).collect().sorted(implicitly[Ordering[Long]].reverse).toSeq.print()

    val stopWatch = new StopWatch

    val grouped = visitsAll.defaultPersist().groupByKeyIntoNestedRDD()

    val res = grouped.mapToScalar[Double] {case (gr, visits) =>
      visits.rdd.defaultPersist()
      val countsPerIP = visits.map((_, 1)).reduceByKey(_+_)
      val numBounces = countsPerIP.filter(_._2 == 1).countNoLoop()
      val numTotalVisitors = visits.distinct().countAssumeNonEmpty()
      LiftedScalar.binaryScalarOp(numBounces, numTotalVisitors)(_.toDouble / _.toDouble)
    }

    res.print()

    stopWatch.done()
  }
}
