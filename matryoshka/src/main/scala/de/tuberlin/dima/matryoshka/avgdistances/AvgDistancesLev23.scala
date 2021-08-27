package de.tuberlin.dima.matryoshka.avgdistances

import Util._
import de.tuberlin.dima.matryoshka.lifting._
import de.tuberlin.dima.matryoshka.util.RDDSplitting._
import de.tuberlin.dima.matryoshka.util.Util._
import de.tuberlin.dima.matryoshka.lifting.LiftedScalar
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer
import scala.util.Random

object AvgDistancesLev23 {

  def main(args: Array[String]): Unit = {

    val skewed = args.length == 5

    val numComp = args(0).toInt
    val numVert = args(1).toInt
    val numEdges = args(2).toLong
    val sampleSize = args(3).toInt
    val exponent =
      if (skewed) {
        args(4).toDouble
      } else {
        0
      }

    implicit val sc: SparkContext = sparkSetup(this)

    val (vertices, edges0) =
      if (skewed) {
        getAvgDistancesSkewedRandomInput(numComp, numVert, numEdges, exponent, sampleSize)
      } else {
        getAvgDistancesRandomInput(numComp, numVert, numEdges)
      }

    val stopWatch = new StopWatch

    /*
    val g = ...
    ConnectedComponents(g).map {(component: Graph) =>
      component.vertices.sample(10).map {source: VID =>
        SSSP(component, source).map(_.distance).average
      }.average
    }
     */

    vertices.defaultPersist()
    // Add the component IDs to the edges
    val edges = vertices.map{case (cid, vid) => (vid, cid)}.join(edges0).map{case (v1, (cid, v2)) => (cid, (v1, v2))}

    edges.defaultPersist()
    val comps = vertices.cogroupIntoRDDs(edges)

    val res = comps.map {case (cid, (vs, es0)) =>
      val rnd = new Random(cid)
      val sources0 = vs.takeSample(withReplacement = false, sampleSize, rnd.nextLong()).toSeq
      //val sources0 = vs.collect().sorted.take(sampleSize).toSeq
      val sources = sc.parallelize(sources0)
      val es = es0.prepareAndPersistForJoin()
      val toAddToDists = ListBuffer[LiftedRDD[Long,(VID,Int)]]()
      val compAvg = sources.mapWithLiftedUDF {src: LiftedScalar[Long, VID] =>
        // Todo: check caching on webui
        val initFront = src.toSingletonRDD
        val initDist = src.unaryOp((_, 0)).toSingletonRDD
        val initStepNum = src.liftingContext.conjure(1)
        val (_, dist, _) = doWhile3RddRddScalar(initFront, initDist, initStepNum,
          (loopContext: LoopContext[Long], front: LiftedRDD[Long,VID], dist: LiftedRDD[Long,(VID,Int)], stepNum: LiftedScalar[Long,Int]) => {
            val newFront =
              front.map(v => (v,())).joinWithNonLiftedAndCached(es)
                .map{case (_, (_, u)) => (u,())}
                .leftAntiJoin(dist)
                .map(_._1)
            newFront.dPersist()
            loopContext.registerForUnpersist(newFront)
            val toAddToDist = newFront.withClosure(stepNum)//.map{case (v: VID, stepNum: Int) => (v, stepNum)} // would be noop
            toAddToDists += toAddToDist
            toAddToDist.rdd.persist(StorageLevel.MEMORY_AND_DISK_SER) // Because of the localCheckpoint
            toAddToDist.rdd.localCheckpoint()
            toAddToDist.rdd.force()
            val newDist = (dist union toAddToDist).coalesce(sc.defaultParallelism)
            val newStepNum = stepNum.unaryOp(_ + 1)
            val cond = front.isEmpty()(loopContext).unaryOp(!_)
            (newFront, newDist, newStepNum, cond)
        })
        dist.map(_._2.toDouble).average
      }.average
      toAddToDists.foreach(_.rdd.unpersist())
      es.unpersist()
      (cid, compAvg)
    }

    //res.print()

    stopWatch.done()
  }

}
