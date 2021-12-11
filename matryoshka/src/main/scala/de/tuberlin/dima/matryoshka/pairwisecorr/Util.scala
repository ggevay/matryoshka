package de.tuberlin.dima.matryoshka.pairwisecorr

import org.apache.spark.SparkContext
import de.tuberlin.dima.matryoshka.util.Util._
import org.apache.spark.rdd.RDD

object Util {

  def getNumRows(path: String)(implicit sc: SparkContext): Int = {
    val metaDataStr = sc.textFile(path + ".mtd").collect.mkString("\n")
    "\"rows\": (.*),".r.findFirstMatchIn(metaDataStr).get.group(1).toLong.toIntOrThrow
  }

  def parseInput(input: RDD[String])(implicit sc: SparkContext): RDD[(Int, Int, Double)] = {
    input.map(ln => {
      val arr = ln.split(' ')
      (arr(0).toInt, arr(1).toInt, arr(2).toDouble)
    })
  }

}
