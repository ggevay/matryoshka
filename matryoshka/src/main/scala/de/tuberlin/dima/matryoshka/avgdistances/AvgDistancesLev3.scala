package de.tuberlin.dima.matryoshka.avgdistances

import Util._
import de.tuberlin.dima.matryoshka.util.Util._
import de.tuberlin.dima.matryoshka.util.RDDSplitting._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer
import scala.util.Random

object AvgDistancesLev3 {

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
      val sources = vs.takeSample(withReplacement = false, sampleSize, new Random(cid).nextLong()).toSeq
      //val sources = vs.collect().sorted.take(sampleSize).toSeq
      val es = es0.prepareAndPersistForJoin()
      val inRes = (cid, sources.map {src: VID =>
        var front = sc.parallelize(Seq(src))
        var dist = sc.parallelize(Seq((src, 0)))
        var stepNum = 1
        var oldDist = sc.emptyRDD[(VID, Int)]
        val toAddToDists = ListBuffer[RDD[(VID,Int)]]()
        do {
          val oldFront = front
          front =
            front.map(v => (v,())).join(es)
              .map{case (_, (_, u)) => (u,())}
              .leftAntiJoin(dist)
              .map(_._1)
          front.defaultPersist()
          val toAddToDist = front.map(v => (v, stepNum))
          toAddToDists += toAddToDist
          toAddToDist.persist(StorageLevel.MEMORY_AND_DISK_SER) // Because of the localCheckpoint
          toAddToDist.localCheckpoint()
          toAddToDist.force()
          oldFront.unpersist()
          oldDist.unpersist()
          oldDist = dist
          dist = (dist ++ toAddToDist).coalesce(sc.defaultParallelism)
          dist.defaultPersist()
          stepNum += 1
        } while (!front.isEmpty())
        //dist.print("dist: ")
        val avg = dist.map(_._2).average
        toAddToDists.foreach(_.unpersist())
        oldDist.unpersist()
        front.unpersist()
        //println("|| " + avg)
        avg
      }.average)
      es.unpersist()
      inRes
    }

    //res.print()

    stopWatch.done()
  }

}
