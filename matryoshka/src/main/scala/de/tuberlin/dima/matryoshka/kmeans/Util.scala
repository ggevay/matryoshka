package de.tuberlin.dima.matryoshka.kmeans

import de.tuberlin.dima.matryoshka.util.Util.randomRDD
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd.RDD
import de.tuberlin.dima.matryoshka.util.Util._

import scala.util.Random

object Util {

  val numSteps = 10

  def randomPoint(rnd: Random, dimension: Integer): DenseVector = {
    Vectors.dense(Seq.fill(dimension)(rnd.nextDouble).toArray).asInstanceOf[DenseVector]
  }

  def add(a: DenseVector, b: DenseVector): DenseVector = {
    Vectors.dense(a.toArray.zip(b.toArray).map{case (x,y)=>x+y}).asInstanceOf[DenseVector]
  }

  def getKMeansRandomInput(numPoints: Long, dim: Int)(implicit sc: SparkContext): RDD[DenseVector] = {
    val points = randomRDD(123, numPoints, rnd => randomPoint(rnd, dim))
    warmup(points)
  }

}
