package de.tuberlin.dima.matryoshka.pairwisecorr

import de.tuberlin.dima.matryoshka.lifting._
import de.tuberlin.dima.matryoshka.pairwisecorr.Util._
import de.tuberlin.dima.matryoshka.util.Util._
import org.apache.spark.SparkContext

object PairwiseCorrLev12 {

  var m: Int = -1 // numRows

  def main(args: Array[String]): Unit = {

    relAssert(args.length == 2)
    val input = args(0)
    val output = args(1)

    implicit val sc: SparkContext = sparkSetup(this)

    deleteHDFSPathIfExists(output)

    val stopWatch = new StopWatch

    m = getNumRows(input)

    val D = parseInput(sc.textFile(input, sc.defaultParallelism))

    D.defaultPersist()
    val cols = D.groupByIntoFlattenedRDD(_._2)
      .map{case (colInd: LiftedScalar[Int, Int], col: LiftedRDD[Int, (Int, Int, Double)]) =>
        (colInd, col.map{case (rowInd, colInd, value) => (rowInd, value)})}

    cols.defaultPersist()

    val crossed = cols cartesian cols

    val result =
      crossed
        .filterOnOuter {case (i, j) => i < j}
        .mapToScalar((ij: LiftedScalar[(Int, Int), (Int, Int)], x: LiftedRDD[(Int, Int), (Int, Double)], y: LiftedRDD[(Int, Int), (Int, Double)]) => {
          LiftedScalar.binaryScalarOp(ij, corr(x, y)){case ((i, j), c) => (i, j, c)}
        })

    result.saveAsTextFile(output)

    stopWatch.done()
  }

  def corr(x: LiftedRDD[(Int, Int), (Int, Double)], y: LiftedRDD[(Int, Int), (Int, Double)]): LiftedScalar[(Int, Int), Double] = {

    def mean(x: LiftedRDD[(Int, Int), (Int, Double)]): LiftedScalar[(Int, Int), Double] = {
      x.map(_._2).sum.unaryOp(_ / m.toDouble)
    }

    def cov(x: LiftedRDD[(Int, Int), (Int, Double)], y: LiftedRDD[(Int, Int), (Int, Double)]): LiftedScalar[(Int, Int), Double] = {
      // https://www.mathworks.com/help/matlab/ref/cov.html
      val meanX = mean(x).defaultPersist()
      val meanY = mean(y).defaultPersist()
      val NZ = (x fullOuterJoin y)
        .withClosure(meanX).withClosure(meanY)
        .map {case (((ind, (l, r)), meanXc), meanYc) => (l.getOrElse(0.0) - meanXc) * (r.getOrElse(0.0) - meanYc)}
        .defaultPersist()
      val numNZ = NZ.countAssumeNonEmpty() // because the inner bags created by groupBy are not empty
      val sumNZ = NZ.sum
      val numZ = numNZ.unaryOp(m - _) //m - numNZ
      val sumZ = LiftedScalar.ternaryScalarOp(numZ, meanX, meanY)(_ * _ * _)
      LiftedScalar.binaryScalarOp(sumNZ, sumZ)(_ + _).unaryOp(_ / (m - 1)) // (sumNZ + sumZ) / (m - 1)
    }

    def sigma(x: LiftedRDD[(Int, Int), (Int, Double)]): LiftedScalar[(Int, Int), Double] = {

      def moment2(x: LiftedRDD[(Int, Int), (Int, Double)]): LiftedScalar[(Int, Int), Double] = {
        val mu = mean(x).defaultPersist()
        val momentNonZero = x.withClosure(mu).map {case ((ind, d), muc) =>
          val a = d - muc
          a * a
        }.sum
        val xCount = x.countAssumeNonEmpty() // because the inner bags created by groupBy are not empty
        val numZero = xCount.unaryOp(m - _) // m - x.count
        val momentZero = LiftedScalar.ternaryScalarOp(mu, mu, numZero)(_ * _ * _) // mu * mu * numZero
        LiftedScalar.binaryScalarOp(momentNonZero, momentZero)(_ + _).unaryOp(_ / m) // (momentNonZero + momentZero) / m
      }

      moment2(x).unaryOp(_ * (m.toDouble / (m.toDouble - 1.0))).unaryOp(a => Math.sqrt(a)) // Math.sqrt(moment2(x) * (m.toDouble / (m.toDouble - 1.0)))
    }

    x.defaultPersist()
    y.defaultPersist()

    LiftedScalar.ternaryScalarOp(cov(x,y), sigma(x), sigma(y))((a, b, c) => a / (b * c)) // cov(x, y) / (sigma(x) * sigma(y))
  }

}
