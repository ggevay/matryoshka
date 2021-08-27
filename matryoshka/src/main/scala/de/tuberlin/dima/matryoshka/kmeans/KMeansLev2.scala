package de.tuberlin.dima.matryoshka.kmeans

import org.apache.spark._
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd._

import scala.util.Random

object KMeansLev2 {

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

    val points = getKMeansRandomInput(numPoints, dim).defaultPersist()

    // --- KMeans

    val t0 = System.nanoTime()

    val result = (0 until runs).map (run => {

      //GCOnAllExecutors(sc)

      val rnd = new Random(run)

      var means = Seq.fill(K)(randomPoint(rnd, dim))
      var sumDistance: Double = -1
      var oldSumDistance: Double = Double.PositiveInfinity
      var assignment: RDD[(DenseVector,DenseVector)] = null
      var it = 0
      do {
        val br = sc.broadcast(means)
        assignment = points.map {p =>
          (br.value.minBy(m => Vectors.sqdist(p, m)), p)
        }.defaultPersist()

        means = assignment.aggregateByKey((Vectors.zeros(dim).asInstanceOf[DenseVector], 0))(
          (sc, v) => (add(sc._1, v), sc._2 + 1),
          (sc1, sc2) => (add(sc1._1, sc2._1), sc1._2 + sc2._2)
        )
          .map{case (_, (s, c)) => Vectors.dense(s.toArray.map(d => d/c)).asInstanceOf[DenseVector]}
          .collect.toSeq

        br.unpersist()

        oldSumDistance = sumDistance
        sumDistance = assignment.map{case (a,b) => Vectors.sqdist(a,b)}.sum()
        assignment.unpersist()
        println("Current sumDistance: " + sumDistance)
        it = it + 1

      //} while (Math.abs(oldSumDistance - sumDistance) > eps)
      } while (it <= numSteps)

      println(s"run ($run) sumDistance: $sumDistance, steps: $it")

      (sumDistance, assignment)
    }).minBy(_._1)

    println(s"Final sumDistance: ${result._1}")
    //result._2.count()
    //result._2.collect().foreach(println)
    //println("Distinct means: " + result._2.map(_._2).distinct().count())

    val t1 = System.nanoTime()
    println("non-warmup-time: " + round2((t1 - t0).asInstanceOf[Double]/1000000000))
    //printManualGCTime()
  }
}
