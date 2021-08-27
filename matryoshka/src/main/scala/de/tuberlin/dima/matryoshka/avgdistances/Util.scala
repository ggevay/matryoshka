package de.tuberlin.dima.matryoshka.avgdistances

import de.tuberlin.dima.matryoshka.util.Util._
import org.apache.commons.math3.distribution.ZipfDistributionGG
import org.apache.commons.math3.random.Well19937c
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

object Util {

  type VID = Long
  type CID = Long

  val seed = 123

  // We assume that the vertices are already tagged with a component ID
  // Returns (vertices, edges), i.e., (RDD[(componentID, vertexID)], RDD[(src, dst)])
  // (The return type assumes Long vertex IDs to be future-proof, but the implementation currently can handle only int-sized vertex- and component IDs)
  // Might give duplicate edges
  def getAvgDistancesRandomInput(numComp: Int, numVert: Int, numEdges: Long)(implicit sc: SparkContext): (RDD[(CID, VID)], RDD[(VID, VID)]) = {
    relAssert(numVert % numComp == 0)
    val compSize = numVert / numComp

    val vertices =
      sc.range(0, numVert)
      .map(v => (v / compSize, v))

    val rndEdgeNum = numEdges - numComp * (compSize - 1)
    relAssert(rndEdgeNum >= 0)

    val rndEdges = warmup(randomRDD(seed, rndEdgeNum, rnd => {
      val comp = rnd.nextInt(numComp)
      val v1 = rnd.nextInt(compSize) + comp * compSize
      val v2 = rnd.nextInt(compSize) + comp * compSize
      (v1.toLong, v2.toLong)
    }))

    val connEdges = vertices.filter{case (_,v) => v % compSize != compSize - 1}.map{case (_,v) => (v, v + 1)}

    val edgesUnioned = (rndEdges union connEdges).coalesce(sc.defaultParallelism)
    val allEdges = edgesUnioned.flatMap {case (u,v) => Seq((u,v),(v,u))}

    (vertices, allEdges)
  }

  def getAvgDistancesSkewedRandomInput(numComp: Int, numVert: Int, numEdges: Long, exponent: Double, sampleSize: Long)(implicit sc: SparkContext): (RDD[(CID, VID)], RDD[(VID, VID)]) = {
    relAssert(numVert % numComp == 0)

    val compIDs = randomRDDWithFillerFactory(seed, numVert, pseed => {
      val zipfDistribution = new ZipfDistributionGG(new Well19937c(pseed), numComp, exponent)
      () => zipfDistribution.sample().toLong
    })

    def indexAndMap[A] = (s: Seq[A]) => s.zipWithIndex.map{case (x,y) => (y.toLong, x)}.toMap

    val compSizesSeq0 = compIDs.map(x => (x,1L)).reduceByKey(_+_).map(_._2).collect().sorted(implicitly[Ordering[Long]].reverse)
    val compSizesSeq1 = compSizesSeq0.map(x => if (x >= sampleSize) x else sampleSize) // We don't want too small components
    val compSizesSeq = compSizesSeq1 ++ Seq.fill(numComp - compSizesSeq1.length)(sampleSize) // Also no empty components

    val compSizesSeqScan = compSizesSeq.scan(0L)(_+_)
    val compSizes = indexAndMap(compSizesSeq)
    val compSizesScan = indexAndMap(compSizesSeqScan)

    val rndEdgeNum = numEdges - (numVert - 1 - (numComp - 1))
    relAssert(rndEdgeNum >= 0)

    val rndEdges = randomRDDWithFillerFactory(seed, rndEdgeNum, pseed => {
      val zipfDistribution = new ZipfDistributionGG(new Well19937c(pseed), numComp, exponent)
      val rng = new Random(pseed)
      () => {
        val comp = zipfDistribution.sample().toLong - 1
        val v1 = rng.nextInt(compSizes(comp).toIntOrThrow).toLong + compSizesScan(comp)
        val v2 = rng.nextInt(compSizes(comp).toIntOrThrow).toLong + compSizesScan(comp)
        (v1, v2)
      }
    })

    val connEdges = sc.parallelize(
      Seq.range(0,numComp)
        .flatMap(comp =>
          Seq.range(compSizesScan(comp), compSizesScan(comp) + compSizes(comp) - 1).map(x => (x, x+1))))

    val edgesUnioned = (rndEdges union connEdges).coalesce(sc.defaultParallelism)
    val allEdges = edgesUnioned.flatMap {case (u,v) => Seq((u,v),(v,u))}

    val vertices = sc.parallelize(
      Seq.range(0,numComp)
        .flatMap(comp =>
          Seq.fill(compSizes(comp).toIntOrThrow)(comp.toLong)
            .zip(Seq.range(compSizesScan(comp), compSizesScan(comp) + compSizes(comp)))))

//    vertices.print("vertices: ")
//    allEdges.print("allEdges: ")

    (warmup(vertices), warmup(allEdges))
  }

  // Without replacement, exact size (except if arr is smaller than sampleSize)
  def sample[T: ClassTag](rnd: Random, sampleSize: Int, arr: Array[T]): Array[T] = {
    if (arr.length <= sampleSize) {
      arr
    } else {
      val res = new Array[T](sampleSize)
      val usedIndices = mutable.Set[Int]()
      for (i <- 0 until sampleSize) {
        var ind: Int = -1
        do {
          ind = rnd.nextInt(arr.length)
        } while (usedIndices.contains(ind))
        usedIndices.add(ind)
        res(i) = arr(ind)
      }
      res
    }
  }

}
