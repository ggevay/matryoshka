package de.tuberlin.dima.matryoshka.pagerank

import de.tuberlin.dima.matryoshka.util.Util.round2
import de.tuberlin.dima.matryoshka.pagerank.Util._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.tuberlin.dima.matryoshka.util.RDDSplitting._
import de.tuberlin.dima.matryoshka.util.Util._

object PageRankLev2 {

  def main(args: Array[String]): Unit = {

    // --- Parse args

    val skewed = args.length == 5 && args(4).isDouble

    val epsilon = args(0).toDouble
    val days = args(1).toInt
    val numVertices = args(2).toInt
    val numEdges = args(3).toLong
    val exponent =
      if (skewed) {
        args(4).toDouble
      } else {
        0
      }

    val d = 0.85

    // --- Spark setup

    implicit val sc: SparkContext = sparkSetup(this)

    // --- Generate input

    val edgesAll =
      if (skewed) {
        getPageRankSkewedRandomInput(days, numVertices, numEdges, exponent)
      } else {
        getPageRankRandomInput(days, numVertices, numEdges)
      }

    // --- PageRank

    println("Starting measurement")

    val t0 = System.nanoTime()

    val grouped = edgesAll.defaultPersist().groupByIntoRDDs(_._1)

    val res = grouped.map{case (day, edges00) =>
      val edges0 = edges00.map{case (day,from,to) => (from,to)}
        .defaultPersist()

      val pages: RDD[Long] = edges0.flatMap{case (a,b) => Seq(a,b)}.distinct()
        .defaultPersist()

      val loopEdges: RDD[(Long, Long)] = pages.map(x => (x,x))

      val edges: RDD[(Long, Long)] = edges0 union loopEdges

      val edgesWithDeg: RDD[(Long, (Long, Int))] = // (src,(dst,srcdeg))
        edges.map{case (a,b) => (a,1)}.reduceByKey(_ + _).join(edges).map{case (k,(v,w)) => (k,(w,v))}
          .prepareAndPersistForJoin()

      val initWeight: Double = 1.0d / pages.count()

      var PR: RDD[(Long, Double)] = pages.map(x => (x, initWeight))
        .defaultPersist()

      edgesWithDeg.force()
      edgesAll.unpersist()
      edges0.unpersist()

      PR.force()
      pages.unpersist()

      var innerDelta: Double = -1
      var it = 0
      do {

        val msgsReduced: RDD[(Long, Double)] =
          PR.join(edgesWithDeg).map{case (src, (value, (dst, srcdeg))) => (dst, value / srcdeg)}.reduceByKeyNoCombiner(_ + _)

        val jump = (1-d) * initWeight
        val newPR: RDD[(Long, Double)] = msgsReduced.map{case (id, v) => (id, d*v+jump)}
          .defaultPersist()

        innerDelta = PR.join(newPR).map{case (k,(oldval, newval)) => scala.math.abs(oldval-newval)}.sum()

        PR.unpersist()
        PR = newPR

        it = it + 1
        println("it: " + it)

      } while (innerDelta > epsilon)

      val c = PR.count()
      //val c = PR.collect()

      PR.unpersist()
      edgesWithDeg.unpersist()

      c
    }

    //println(res)
    println(res.length)

    val t1 = System.nanoTime()
    println("non-warmup-time: " + round2((t1 - t0).asInstanceOf[Double]/1000000000))
  }

}
