package org.apache.spark

import org.apache.spark.rdd.{CartesianPartition, CartesianRDD, RDD}

import scala.reflect.ClassTag

class CartesianRDDHackedLocality[T: ClassTag, U: ClassTag](
                                                            sc: SparkContext,
                                                            val rdd1a : RDD[T],
                                                            val rdd2a : RDD[U]) extends CartesianRDD(sc, rdd1a, rdd2a) {

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[CartesianPartition]

    val l1 = rdd1.preferredLocations(currSplit.s1)
    val l2 = rdd2.preferredLocations(currSplit.s2)

    val ret = rdd2.preferredLocations(currSplit.s2)

    //val ret = Seq()

    ret
  }

}
