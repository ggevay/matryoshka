package de.tuberlin.dima.matryoshka.util

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}
import Util._

import scala.reflect.ClassTag

object RDDSplitting {

  implicit class groupByIntoRDDsAble[T: ClassTag](val in: RDD[T]) {

    def groupByIntoRDDs[K: ClassTag](f: T => K): Seq[(K, RDD[T])] = {
      relAssert(in.getStorageLevel != StorageLevel.NONE, "Input of groupByIntoRDDs is not cached!")
      val inter = GroupByIntoRDDsIntermediate(in)(f)
      val keys = in.map(f).distinct(1).collect()
      keys.map(k => (k, inter.getRDDForKey(k)))
    }
  }

  implicit class PairRDDExtensions3[K: ClassTag, V: ClassTag](rdd: RDD[(K,V)]) {

    def cogroupIntoRDDs[W: ClassTag](other: RDD[(K, W)]): Seq[(K, (RDD[V], RDD[W]))] = {
      relAssert(rdd.getStorageLevel != StorageLevel.NONE && other.getStorageLevel != StorageLevel.NONE, "Input of cogroupIntoRDDs is not cached!")
      val interThis = GroupByIntoRDDsIntermediate(rdd)(_._1)
      val interOther = GroupByIntoRDDsIntermediate(other)(_._1)
      val keys = (rdd.map(_._1) union other.map(_._1)).distinct(1).collect()
      keys.map(k => (k, (interThis.getRDDForKey(k).map(_._2), interOther.getRDDForKey(k).map(_._2))))
    }

    def groupByKeyIntoRDDs(): Seq[(K, RDD[V])] = {
      rdd.groupByIntoRDDs(_._1).map {case (k, vs) => (k, vs.map(_._2))}
    }
  }

  class GroupByIntoRDDsIntermediate[K: ClassTag, T: ClassTag](private val partitionsRDD: RDD[Map[K, Array[T]]])
    extends RDD[T](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

    override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

    override def compute(part: Partition, context: TaskContext): Iterator[T] = {
      firstParent[Map[K, Seq[T]]].iterator(part, context).next().values.flatten.iterator
    }

    def getRDDForKey(k: K): RDD[T] = {
      partitionsRDD.flatMap(m => m.getOrElse[Array[T]](k, new Array[T](0)))
    }
  }

  object GroupByIntoRDDsIntermediate {
    def apply[K: ClassTag, T: ClassTag](in: RDD[T])(f: T => K): GroupByIntoRDDsIntermediate[K, T] = {
      val partitions = in.mapPartitions(iter => Iterator(
        //iter.toSeq.groupBy(f)
        iter.toArray.groupBy(f)
      ),
        preservesPartitioning = false)
        .persist(StorageLevel.MEMORY_AND_DISK) // Btw. we have a problem if this spills to disk, but there is nothing better we can do here
      new GroupByIntoRDDsIntermediate[K, T](partitions)
    }
  }
}
