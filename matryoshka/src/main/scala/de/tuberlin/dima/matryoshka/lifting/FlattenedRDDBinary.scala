package de.tuberlin.dima.matryoshka.lifting

import de.tuberlin.dima.matryoshka.util.Util.LBLSJoinStrategy.{Broadcast, Repartition}
import org.apache.spark.rdd.RDD
import de.tuberlin.dima.matryoshka.util.Util._
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

// Simulates an RDD[(O,RDD[I1],RDD[I2])], with lifting id L
class FlattenedRDDBinary[L: ClassTag, O: ClassTag, I1: ClassTag, I2: ClassTag]
    (val outer: LiftedScalar[L,O], val inner1: LiftedRDD[L,I1], val inner2: LiftedRDD[L,I2])(implicit val liftingContext: LiftingContext[L]) {

  def mapToScalar[R: ClassTag](liftedMapFunc: (LiftedScalar[L,O], LiftedRDD[L,I1], LiftedRDD[L, I2]) => LiftedScalar[L,R]): RDD[R] = {
    liftedMapFunc(outer, inner1, inner2).unliftToRDD
  }

  def filterOnOuter(filterFunc: O => Boolean)(implicit sc: SparkContext): FlattenedRDDBinary[L,O,I1,I2] = {
    val outerFiltered = outer.rdd.filterValues(filterFunc).defaultPersist()
    val remainingLiftIDs = outerFiltered.map(x => (x._1, ())).defaultPersist()

    // We need to do the following joins, but with an appropriate join strategy
//    val inner1Filtered = (inner1.rdd join remainingLiftIDs) map {case (l, (i1, _)) => (l, i1)}
//    val inner2Filtered = (inner2.rdd join remainingLiftIDs) map {case (l, (i2, _)) => (l, i2)}

    val (inner1FilteredRaw, inner2FilteredRaw) = decideLBLSJoinStrategy(remainingLiftIDs.count(), sc.defaultParallelism) match {
      case Broadcast =>
        val br = collectForBroadcast(remainingLiftIDs)
        (
          inner1.rdd broadcastJoinRightPrimaryKey br,
          inner2.rdd broadcastJoinRightPrimaryKey br
        )
      case Repartition => (
        inner1.rdd join remainingLiftIDs,
        inner2.rdd join remainingLiftIDs
      )
    }

    val inner1Filtered = inner1FilteredRaw map {case (l, (x, _)) => (l, x)}
    val inner2Filtered = inner2FilteredRaw map {case (l, (x, _)) => (l, x)}

    new FlattenedRDDBinary[L,O,I1,I2](
      outerFiltered, inner1Filtered, inner2Filtered
    )(
      implicitly, implicitly, implicitly, implicitly, new LiftingContext[L](outerFiltered.map(_._1), liftingContext.outerLiftingContext)
    )
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