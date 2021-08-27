package de.tuberlin.dima.matryoshka.lifting

import org.apache.spark.rdd.RDD
import LiftedScalar._
import LiftedRDD._
import de.tuberlin.dima.matryoshka.util.Util._
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

// Simulates an RDD[(O,RDD[I])], with lifting id L
class FlattenedRDD[L: ClassTag, O: ClassTag, I: ClassTag](val outer: LiftedScalar[L,O], val inner: LiftedRDD[L,I])(implicit val liftingContext: LiftingContext[L]) {

  def map[O2: ClassTag, I2: ClassTag](liftedMapFunc: (LiftedScalar[L,O], LiftedRDD[L,I]) => (LiftedScalar[L,O2], LiftedRDD[L,I2])): FlattenedRDD[L,O2,I2] = {
    liftedMapFunc(outer, inner)
  }

  def mapToScalar[O2: ClassTag, I2: ClassTag](liftedMapFunc: (LiftedScalar[L,O], LiftedRDD[L,I]) => (LiftedScalar[L,O2], LiftedScalar[L,I2])): RDD[(O2,I2)] = {
    val (s1, s2) = liftedMapFunc(outer, inner)
    (s1 merge s2).unliftToRDD
  }

  def mapToScalar[R: ClassTag](liftedMapFunc: (LiftedScalar[L,O], LiftedRDD[L,I]) => LiftedScalar[L,R]): RDD[R] = {
    liftedMapFunc(outer, inner).unliftToRDD
  }

}

object FlattenedRDD {

  import scala.language.implicitConversions
  private[lifting] implicit def fromLayers[L: ClassTag, O: ClassTag, I: ClassTag](layers: (LiftedScalar[L,O], LiftedRDD[L,I]))(implicit lc: LiftingContext[L]): FlattenedRDD[L,O,I] =
    new FlattenedRDD(layers._1, layers._2)

  // From a PairRDD
  def fromGrouping[K: ClassTag, V: ClassTag](r: RDD[(K,V)]): FlattenedRDD[K,K,V] = {
    relAssert(r.getStorageLevel != StorageLevel.NONE)
    val base = r.keys.distinct().autoCoalesceAndPersist()
    implicit val ilc: LiftingContext[K] = new LiftingContext(base)
    new FlattenedRDD(
      base.map(x => (x,x)),
      r
    )
  }

  // Use a keyFunc
  def fromGrouping[K: ClassTag, T: ClassTag](r: RDD[T], keyFunc: T => K): FlattenedRDD[K,K,T] = {
    relAssert(r.getStorageLevel != StorageLevel.NONE)
    val base = r.map(keyFunc).distinct().autoCoalesceAndPersist()
    implicit val ilc: LiftingContext[K] = new LiftingContext(base)
    new FlattenedRDD(
      base.map(x => (x,x)),
      r.map(x => (keyFunc(x), x))
    )
  }

}