package de.tuberlin.dima.matryoshka.lifting

import org.apache.spark.rdd.RDD
import de.tuberlin.dima.matryoshka.util.Util._

import scala.reflect.ClassTag

// Simulates an RDD[(O,RDD[I1],RDD[I2])], with lifting id L
class FlattenedRDDBinary[L: ClassTag, O: ClassTag, I1: ClassTag, I2: ClassTag]
    (val outer: LiftedScalar[L,O], val inner1: LiftedRDD[L,I1], val inner2: LiftedRDD[L,I2])(implicit val liftingContext: LiftingContext[L]) {

  def mapToScalar[R: ClassTag](liftedMapFunc: (LiftedScalar[L,O], LiftedRDD[L,I1], LiftedRDD[L, I2]) => LiftedScalar[L,R]): RDD[R] = {
    liftedMapFunc(outer, inner1, inner2).unliftToRDD
  }

}

object FlattenedRDDBinary {

  import scala.language.implicitConversions
  private[lifting] implicit def fromLayers[L: ClassTag, O: ClassTag, I1: ClassTag, I2: ClassTag]
      (layers: (LiftedScalar[L,O], LiftedRDD[L,I1], LiftedRDD[L,I2]))(implicit lc: LiftingContext[L]): FlattenedRDDBinary[L,O,I1,I2] =
    new FlattenedRDDBinary(layers._1, layers._2, layers._3)

  // From two PairRDDs
  def fromGrouping[K: ClassTag, V: ClassTag, W: ClassTag](r1: RDD[(K,V)], r2: RDD[(K,W)]): FlattenedRDDBinary[K,K,V,W] = {
    val base = (r1.keys union r2.keys).distinct().autoCoalesceAndPersist()
    implicit val ilc: LiftingContext[K] = new LiftingContext(base)
    new FlattenedRDDBinary(
      base.map(x => (x,x)),
      r1,
      r2
    )
  }

}