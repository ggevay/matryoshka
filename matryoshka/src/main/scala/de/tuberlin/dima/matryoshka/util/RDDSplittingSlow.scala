package de.tuberlin.dima.matryoshka.util

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

object RDDSplittingSlow {
  implicit class groupByIntoRDDsAble[T](val in: RDD[T]) {
    def groupByIntoRDDs[K: ClassTag](f: T => K): Seq[(K, RDD[T])] = {
      in.persist(StorageLevel.MEMORY_ONLY)
      val keys = in.map(f).distinct(1).collect()
      keys.map(k => (k, in.filter(x => f(x) == k)))
    }
  }
}
