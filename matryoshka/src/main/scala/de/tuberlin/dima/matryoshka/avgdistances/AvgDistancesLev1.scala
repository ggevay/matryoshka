package de.tuberlin.dima.matryoshka.avgdistances

import Util._
import de.tuberlin.dima.matryoshka.util.Util._
import org.apache.spark.SparkContext

import scala.util.Random

object AvgDistancesLev1 {

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

    val comps = vertices.cogroup(edges)

    val res = comps.map {case (cid, (vs0, es0)) =>
      val vs = vs0.toArray
      val es = es0.toSeq
      val rnd = new Random(cid)
      val sources = sample(rnd, sampleSize, vs).toSeq
      //val sources = vs.sorted.take(sampleSize).toSeq
      (cid, sources.map {src: VID =>
        var front = Seq(src)
        var dist = Seq((src, 0))
        var stepNum = 1
        do {
          val newFront =
            front.map(v => (v,())).join(es)
              .map{case (_, (_, u)) => (u,())}
              .leftAntiJoin(dist)
              .map(_._1)
          dist = dist ++ newFront.map(v => (v, stepNum))
          front = newFront
          stepNum += 1
        } while (front.nonEmpty)
        //dist.print("dist: ")
        val avg = dist.map(_._2).average
        //println("|| " + avg)
        avg
      }.average)
    }

    res.force()

    //res.print()

    stopWatch.done()
  }

}
