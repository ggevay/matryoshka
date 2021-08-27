package de.tuberlin.dima.matryoshka.pagerank

import de.tuberlin.dima.matryoshka.util.Util._
import de.tuberlin.dima.matryoshka.pagerank.Util._
import org.apache.spark.{SparkConf, SparkContext}

object PageRankLev1 {

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

    val grouped = edgesAll.groupBy(_._1)

    val res = grouped.map{case (day, edges00) =>
      val edges0 = edges00.toSeq.map{case (day,from,to) => (from,to)}

      val pages: Seq[Long] = edges0.flatMap{case (a,b) => Seq(a,b)}.distinct

      val loopEdges: Seq[(Long, Long)] = pages.map(x => (x,x))

      val edges: Seq[(Long, Long)] = edges0 union loopEdges

      val edgesWithDeg: Seq[(Long, (Long, Int))] = // (src,(dst,srcdeg))
        edges.map{case (a,b) => (a,1)}.reduceByKey(_ + _).join(edges).map{case (k,(v,w)) => (k,(w,v))}

      val initWeight: Double = 1.0d / pages.count()

      var PR: Seq[(Long, Double)] = pages.map(x => (x, initWeight))

      var innerDelta: Double = -1
      var it = 0
      do {

        val msgsReduced: Seq[(Long, Double)] =
          PR.join(edgesWithDeg).map{case (src, (value, (dst, srcdeg))) => (dst, value / srcdeg)}.reduceByKey(_ + _)

        val jump = (1-d) * initWeight
        val newPR: Seq[(Long, Double)] = msgsReduced.map{case (id, v) => (id, d*v+jump)}

        innerDelta = PR.join(newPR).map{case (k,(oldval, newval)) => scala.math.abs(oldval-newval)}.sum

        PR = newPR

        it = it + 1
        println("it: " + it)

      } while (innerDelta > epsilon)

      val c = PR.count()
      //val c = PR.collect()

      c
    }

    //println(res.collect.mkString(", "))
    println(res.count())

    val t1 = System.nanoTime()
    println("non-warmup-time: " + round2((t1 - t0).asInstanceOf[Double]/1000000000))
  }

}
