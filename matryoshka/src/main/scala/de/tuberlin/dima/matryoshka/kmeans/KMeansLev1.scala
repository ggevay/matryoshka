package de.tuberlin.dima.matryoshka.kmeans

import org.apache.spark._
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd._

import scala.util.Random

object KMeansLev1 {

  import de.tuberlin.dima.matryoshka.util.Util._
  import Util._

  def main(args: Array[String]): Unit = {

    // --- Parse args

    //val million = 1000*1000

    val numPoints = args(0).toLong
    val K         = args(1).toInt
    val dim       = args(2).toInt
    val runs      = args(3).toInt

    val eps = 1

    // --- Spark setup

    implicit val sc: SparkContext = sparkSetup(this)

    // --- Generate input

    val pointsColl = getKMeansRandomInput(numPoints, dim).collect()

    // --- KMeans

    val t0 = System.nanoTime()

    val pointsBr = sc.broadcast(pointsColl)

    val result = sc.range(0, runs).map (run => {
      val rnd = new Random(run)

      val points = pointsBr.value

      var means = Seq.fill(K)(randomPoint(rnd, dim))
      var sumDistance: Double = -1
      var oldSumDistance: Double = Double.PositiveInfinity
      var assignment: Seq[(DenseVector,DenseVector)] = null
      var it = 0
      do {
        assignment = points.map {p =>
          (means.minBy(m => Vectors.sqdist(p, m)), p)
        }

        means = assignment.aggregateByKey((Vectors.zeros(dim).asInstanceOf[DenseVector], 0))(
          (sc, v) => (add(sc._1, v), sc._2 + 1),
          (sc1, sc2) => (add(sc1._1, sc2._1), sc1._2 + sc2._2)
        )
          .map{case (_, (s, c)) => Vectors.dense(s.toArray.map(d => d/c)).asInstanceOf[DenseVector]}

        oldSumDistance = sumDistance
        sumDistance = assignment.map{case (a,b) => Vectors.sqdist(a,b)}.sum
        println("Current sumDistance: " + sumDistance)
        it = it + 1

      //} while (Math.abs(oldSumDistance - sumDistance) > eps)
      } while (it <= numSteps)

      println(s"run ($run) sumDistance: $sumDistance, steps: $it")

      (sumDistance, null) // assignment.toArray // No need to return the assignment for now (we could return it by writing a custom minBy that returns a singleton RDD)
    }).minBy(_._1)

    println(s"Final sumDistance: ${result._1}")
    //result._2.count()
    //result._2.collect().foreach(println)
    //println("Distinct means: " + result._2.map(_._2).distinct.count())

    val t1 = System.nanoTime()
    println("non-warmup-time: " + round2((t1 - t0).asInstanceOf[Double]/1000000000))
  }
}
