package de.tuberlin.dima.matryoshka.pairwisecorr

import de.tuberlin.dima.matryoshka.util.Util._
import org.apache.spark.SparkContext
import Util._
import org.apache.spark.storage.StorageLevel

object PairwiseCorrLev1 {

  var m: Int = -1 // numRows

  def main(args: Array[String]): Unit = {

    relAssert(args.length == 2)
    val input = args(0)
    val output = args(1)

    implicit val sc: SparkContext = sparkSetup(this)

    deleteHDFSPathIfExists(output)

    val stopWatch = new StopWatch

    m = getNumRows(input)

    val D = parseInput(sc.textFile(input))

    val cols = D.groupBy(_._2)
      .map{case (colInd, col: Iterable[(Int, Int, Double)]) =>
        (colInd, col.map{case (rowInd, colInd, value) => (rowInd, value)})}

    cols.defaultPersist()

    val crossed = (cols cartesian cols).coalesce(sc.defaultParallelism)

    val result =
      crossed
        .filter {case ((i, x), (j, y)) => i < j}
        .map {case ((i, x), (j, y)) => (i, j, corr(x.toSeq, y.toSeq))}

    result.saveAsTextFile(output)

    stopWatch.done()
  }

  def corr(x: Seq[(Int, Double)], y: Seq[(Int, Double)]): Double = {

    def mean(x: Seq[(Int, Double)]): Double = {
      x.map(_._2).sum / m.toDouble
    }

    def cov(x: Seq[(Int, Double)], y: Seq[(Int, Double)]): Double = {
      // https://www.mathworks.com/help/matlab/ref/cov.html
      val meanX = mean(x)
      val meanY = mean(y)
      val NZ = (x fullOuterJoin y)
        .map {case (ind, (l, r)) => (l.getOrElse(0.0) - meanX) * (r.getOrElse(0.0) - meanY)}
      val numNZ = NZ.count()
      val sumNZ = NZ.sum
      val numZ = m - numNZ
      val sumZ = numZ * meanX * meanY
      (sumNZ + sumZ) / (m - 1)
    }

    def sigma(x: Seq[(Int, Double)]): Double = {

      def moment2(x: Seq[(Int, Double)]): Double = {
        val mu = mean(x)
        val momentNonZero = x.map{case (ind, d) =>
          val a = d - mu
          a * a
        }.sum
        val numZero = m - x.size
        val momentZero = mu * mu * numZero
        (momentNonZero + momentZero) / m
      }

      Math.sqrt(moment2(x) * (m.toDouble / (m.toDouble - 1.0)))
    }

    cov(x, y) / (sigma(x) * sigma(y))
  }

}
