package de.tuberlin.dima.matryoshka.bouncerate

import de.tuberlin.dima.matryoshka.util.Util._
import org.apache.commons.math3.distribution.{ZipfDistribution, ZipfDistributionGG}
import org.apache.commons.math3.random.Well19937c
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Random

object Util {

  def bounceRatePerGroup_manually_flattened(visits: RDD[(Int, String)]): RDD[(Int,Double)] = {
    val countsPerDayAndIp = visits.map((_, 1)).reduceByKey(_+_)
    val numBouncesPerDay = countsPerDayAndIp.filter(_._2 == 1).map{case ((day,ip),count) => (day,1)}.reduceByKey(_+_)
    val numTotalVisitorsPerDay = visits.distinct().map{case (day,ip) => (day,1)}.reduceByKey(_+_)
    (numBouncesPerDay join numTotalVisitorsPerDay).map{case (day, (numBounces, numTotalVisitors)) => (day, numBounces.toDouble / numTotalVisitors.toDouble)}
  }

  def getBounceRateRandomInput(totalSize: Long, numGroups: Int)(implicit sc: SparkContext): RDD[(Int, Long)] = {
    relAssert(totalSize % numGroups == 0)
    val groupSize = totalSize / numGroups
    warmup(randomRDD(123, totalSize, rnd => (rnd.nextInt(numGroups), (rnd.nextDouble() * groupSize).toLong)))
  }

  def getBounceRateSkewedRandomInput(totalSize: Long, numGroups: Int, exponent: Double)(implicit sc: SparkContext): RDD[(Int, Long)] = {
    relAssert(totalSize % numGroups == 0)
    val groupSize = totalSize / numGroups
    warmup(randomRDDWithFillerFactory(123, totalSize, pseed => {
      val zipfDistribution = new ZipfDistributionGG(new Well19937c(pseed), numGroups, exponent)
      val rng = new Random(pseed)
      () => (zipfDistribution.sample(), (rng.nextDouble() * groupSize).toLong)
    }))
  }
}
