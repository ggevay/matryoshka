package de.tuberlin.dima.matryoshka.pagerank

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.tuberlin.dima.matryoshka.util.Util._
import org.apache.commons.math3.distribution.ZipfDistributionGG
import org.apache.commons.math3.random.Well19937c

import scala.util.Random

object Util {

  def getPageRankRandomInput(days: Int, numVertices: Int, numEdges: Long)(implicit sc: SparkContext): RDD[(Int, Long, Long)] = {
    warmup(randomRDD(123, numEdges, rnd => (rnd.nextInt(days), rnd.nextInt(numVertices), rnd.nextInt(numVertices))))
  }

  def getPageRankSkewedRandomInput(days: Int, numVertices: Int, numEdges: Long, exponent: Double)(implicit sc: SparkContext): RDD[(Int, Long, Long)] = {
    warmup(randomRDDWithFillerFactory(123, numEdges, pseed => {
      val zipfDistribution = new ZipfDistributionGG(new Well19937c(pseed), days, exponent)
      val rng = new Random(pseed)
      () => (zipfDistribution.sample(), rng.nextInt(numVertices), rng.nextInt(numVertices))
    }))
  }
}
