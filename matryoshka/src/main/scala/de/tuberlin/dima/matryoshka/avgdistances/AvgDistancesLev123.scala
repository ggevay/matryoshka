package de.tuberlin.dima.matryoshka.avgdistances

import Util._
import de.tuberlin.dima.matryoshka.lifting.{LiftedRDD, LiftedScalar}
import de.tuberlin.dima.matryoshka.lifting._
import de.tuberlin.dima.matryoshka.util.Util._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Random

object AvgDistancesLev123 {

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

    val comps = vertices.cogroupIntoNestedRDD(edges)

    val res: RDD[(CID, Double)] = comps.mapToScalar ((cid: LiftedScalar[CID,CID], vs: LiftedRDD[CID,VID], es0: LiftedRDD[CID,(VID,VID)]) => {
      val sources = vs.sample(withReplacement = false, sampleSize, new Random(42).nextLong())
      val es = es0.prepareAndPersistForJoin()
      //val sources = vs.sorted.take(sampleSize).toSeq
      val compAvg: LiftedScalar[CID,Double] = sources.mapWithLiftedUDF {src =>
        val initFront = src.toSingletonRDD
        val initDist = src.unaryOp((_, 0)).toSingletonRDD
        val initStepNum = src.liftingContext.conjure(1)
        val (_, dist, _) = doWhile3RddRddScalar(initFront, initDist, initStepNum,
          (loopContext: LoopContext[(CID, Long)], front: LiftedRDD[(CID, Long),VID], dist: LiftedRDD[(CID, Long),(VID,Int)], stepNum: LiftedScalar[(CID, Long),Int]) => {
            val newFront =
              front.map(v => (v,())).joinWithLessLiftedAndCached(es)
                .map{case (_, (_, u)) => (u,())}
                .leftAntiJoin(dist)
                .map(_._1)
            newFront.dPersist()
            loopContext.registerForUnpersist(newFront)
            val toAddToDist = newFront.withClosure(stepNum)//.map{case (v: VID, stepNum: Int) => (v, stepNum)} // would be noop
            //toAddToDists += toAddToDist // No need for this, as we run the loop only once
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
      LiftedScalar.binaryScalarOp(cid, compAvg)((_,_))
    })

    res.force()

    //res.print()

    stopWatch.done()
  }

}
