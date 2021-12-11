package de.tuberlin.dima.matryoshka.pagerank

import de.tuberlin.dima.matryoshka.pagerank.Util._
import de.tuberlin.dima.matryoshka.util.Util._
import de.tuberlin.dima.matryoshka.lifting._
import de.tuberlin.dima.matryoshka.lifting.LiftedScalar._
import de.tuberlin.dima.matryoshka.lifting.LiftedRDD._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object PageRankLev12 {

  def main(args: Array[String]): Unit = {

    // --- Parse args

    relAssert(args.length < 6) // Cannot be both skewed and joinStrategyGiven

    val skewed = args.length == 5 && args(4).isDouble
    val joinStrategyGiven = args.length == 5 && !args(4).isDouble

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

    if (joinStrategyGiven)
      LBLSJoinStrategyConfig = LBLSJoinStrategy.withName(args(4))

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

//    // Group sizes:
//    //println(visitsAll.map{case (k,_) => (k,1L)}.reduceByKey(_+_).map(_._2).max())
//    edgesAll.map{case (k,_,_) => (k,1L)}.reduceByKey(_+_).map(_._2).collect().sorted(implicitly[Ordering[Long]].reverse).toSeq.print()

    // --- PageRank

    println("Starting measurement")

    val t0 = System.nanoTime()



    val grouped = edgesAll.defaultPersist().groupByIntoFlattenedRDD(_._1)

    val res = grouped.mapToScalar[Long]{case (day, edges00) =>

      val edges0 = edges00.map{case (day,from,to) => (from,to)}
      edges0.defaultPersist()

      val pages = edges0.flatMap{case (a,b) => Seq(a,b)}.distinct(false)
      pages.defaultPersist()

      val loopEdges = pages.map(x => (x,x))

      // Even though edges is also used twice (in edgesWithDeg), we would gain very little from persisting it,
      // because edges0 is cached, and loopEdges one-to-one depends on pages and pages is cached, and union is cheap
      val edges = edges0 union loopEdges

      // (src,(dst,srcdeg))
      val edgesWithDeg = edges
        .map{case (a,b) => (a,1)}.reduceByKey(_ + _, mapSideCombine = false).join(edges).map{case (k,(v,w)) => (k,(w,v))}
        .prepareAndPersistForJoin()
      //edgesWithDeg.dPersist()

      //day.dPersist()
      //val initWeight: Double = 1.0d / pages.count()
      //val initWeight = binaryScalarOp(day.unaryOp(_ => 1.0d), pages.count())(_/_)
      val initWeight = pages.countAssumeNonEmpty().unaryOp(1.0d / _)

      var jumpBr: Broadcast[Map[Int, Double]] = null
      val jump = initWeight.unaryOp(w => (1-d)*w)
      import LBLSJoinStrategy._
      decideLBLSJoinStrategy(jump) match {
        case Broadcast => jumpBr = jump.prepareForBroadcast()
        case Repartition =>
          jump.defaultPersist()
          jump.force()
      }

      //var PR = pages.map(x => (x, initWeight))
      // initWeight should, in theory, be unpersisted later, but it's small, so it's ok
      val initPR = pages.withClosure(initWeight)//.map{case (x, w) => (x, w)}
      initPR.defaultPersist()

      edgesWithDeg.force()
      edgesAll.unpersist()
      edges0.rdd.unpersist()

      initPR.force()
      pages.rdd.unpersist()

      //val liftedEpsilon = day.unaryOp(_ => epsilon)

      val finalPR = LiftedRDD.doWhile1(initPR, (loopContext: LoopContext[Int], PR: LiftedRDD[Int, (Long, Double)]) => {

        val msgsReduced =
          PR.joinWithCached(edgesWithDeg).map{case (src, (value, (dst, srcdeg))) => (dst, value / srcdeg)}.reduceByKey(_ + _, mapSideCombine = false)

        val msgsReducedWithClosure =
          decideLBLSJoinStrategy(jump) match  {
            case Broadcast => msgsReduced.withClosureCached(jumpBr)
            case Repartition => msgsReduced.withClosure(jump) // also cached, but no different function needed between cached and non-cached in this case
          }
        val newPR = msgsReducedWithClosure.map{case ((id, v),jump) => (id, d*v+jump)}
        newPR.defaultPersist() // Needed, because the merge that creates bodyOut_more depends on this through both its inputs
        loopContext.registerForUnpersist(newPR.rdd)

        val innerDelta = PR.join(newPR).map{case (k,(oldval, newval)) => scala.math.abs(oldval-newval)}.sum

        //PR.unpersist()

        //val cond = binaryScalarOp(innerDelta, liftedEpsilon)(_>_)
        val cond = innerDelta.unaryOp(d => d > epsilon)

        // I think this would not be needed, because doWhile persists and unpersists bodyIn anyway (which is this RDD)
        // Note that this is not the same RDD as newPR, because there is the filtering and merging and whatnot in doWhile
        loopContext.registerForUnpersist(PR.rdd)

        (newPR, cond)
      })

      val c = finalPR.countAssumeNonEmpty()
      //val c = PR.collect()

      //(day, c)
      c
    }

    //println(res.collect.mkString(", "))
    println(res.count())

    val t1 = System.nanoTime()
    println("non-warmup-time: " + round2((t1 - t0).asInstanceOf[Double]/1000000000))
  }

}
