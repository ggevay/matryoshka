package de.tuberlin.dima.matryoshka.lifting

import java.nio.ByteBuffer

import org.apache.spark.{Partitioner, SparkEnv}
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd._
import de.tuberlin.dima.matryoshka.util.Util._
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

class LiftedRDD[L: ClassTag, T: ClassTag](val rdd: RDD[(L,T)])(implicit val liftingContext: LiftingContext[L]) {

  val ilc: LiftingContext[L] = liftingContext

  def map[U: ClassTag](f: T => U): LiftedRDD[L,U] = {
    rdd.mapValues(f)
  }

  def filter(f: T => Boolean): LiftedRDD[L, T] = {
    rdd.filterValues(f)
  }

  def mapWithLiftedUDF[U: ClassTag](liftedMapFunc: LiftedScalar[(L, Long), T] => LiftedScalar[(L, Long), U]): LiftedRDD[L,U] = {
    liftedMapFunc(liftToScalar).unliftToLiftedRDD
  }

  private[lifting] def liftToScalar: LiftedScalar[(L, Long), T] = {
    val rddWithLiftIDs = rdd.zipWithUniqueId().map {case ((l1, t), l2) => ((l1, l2), t)}.autoCoalesceAndPersist()
    implicit val innerLC: LiftingContext[(L, Long)] = new LiftingContext(rddWithLiftIDs.map(_._1), ilc)
    new LiftedScalar(rddWithLiftIDs)
  }

  // It's not an action!
  def collect: LiftedScalar[L,Seq[T]] = { // (should be Array instead of Seq, officially)
    //this.rdd.groupByKey(lc.defaultLiftedScalarParallelism).mapValues(_.toSeq)

    this.rdd.groupByKey(liftingContext.defaultLiftedScalarParallelism).mapValues(_.toSeq)
  }

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): LiftedRDD[L,U] = {
    rdd.flatMapValues(f)
  }

  def distinct(mapSideCombine: Boolean = true): LiftedRDD[L,T] = {
    //rdd.distinct()

    rdd.partitioner match {
      case Some(_)  => rdd.distinct() // This does it without a shuffle
      case _ => map(x => (x, null)).reduceByKey((x, _) => x, mapSideCombine).map(_._1)
    }
  }

  def union(other: LiftedRDD[L,T]): LiftedRDD[L,T] = {
    rdd union other.rdd
  }

  def sample(withReplacement: Boolean = false, num: Int, seed: Long): LiftedRDD[L,T] = {
    rdd.defaultPersist()
    val fractions = rdd.countByKey()
      .mapValues(groupSize => Math.min(1.0, num.toDouble / groupSize.toDouble))
      .toSeq.toMap // No idea why, but the Map returned by mapValues is not serializable, so this works around that
    rdd.sampleByKeyExact(withReplacement, fractions, seed)
  }

  // Just a count, but assumes the bag is not empty (for any lift ID)
  def countAssumeNonEmpty(): LiftedScalar[L,Long] = {
    this.map(_ => 1L).rdd.reduceByKey(_+_, ilc.defaultLiftedScalarParallelism)
  }

  def countNoLoop(): LiftedScalar[L,Long] = {
    addEmpties(0L)(this.map(_ => 1L).rdd.reduceByKey(_+_, ilc.defaultLiftedScalarParallelism))
  }

  def isEmptyNoLoop: LiftedScalar[L,Boolean] = {
    isEmpty()(null)
  }

  def isEmpty()(implicit loopContext: LoopContext[L]): LiftedScalar[L,Boolean] = {
    addEmpties(true)(rdd.map(_._1).distinct(liftingContext.defaultLiftedScalarParallelism).map(l => (l, false)))
  }

  private def resolveBase(loopContext: LoopContext[L]): RDD[L] = {
    if (loopContext != null)
      loopContext.base
    else
      liftingContext.base
  }

  private def addEmpties[W: ClassTag](valForEmpty: W)(nonEmpties: RDD[(L,W)])(implicit loopContext: LoopContext[L] = null): LiftedScalar[L,W] = {
    val base = resolveBase(loopContext).map(x => (x,()))
    nonEmpties.cogroup(base, numPartitions = liftingContext.defaultLiftedScalarParallelism).map {
      case (lid, (Seq(nonEmpty), _)) => (lid, nonEmpty)
      case (lid, (Seq(), Seq(b))) => (lid, valForEmpty)
    }
  }

  def replicate[C: ClassTag](s: LiftedScalar[L,C]): LiftedRDD[L,(T,C)] = {
    //We just need to do  rdd join s.rdd
    // However, there are two issues if we do the join carelessly:
    //  - If s is small, then we don't want to shuffle the whole left rdd, but do a broadcast join instead.
    //  - A more critical issue is that the repartition join makes a mess if the join key cardinality is
    //    lower than the default parallelism, because then we end up with a result rdd that has only that many non-empty
    //    partitions. And this can very often happen here, when numInner is small.
    // So, ideally, an optimizer would look at s.rdd, and if it's large (L is primary key, so enough to check the size)
    // then repartition join, otherwise broadcast join. And the nice thing is that we can put the necessary info for
    // this into the LiftingContext, and then no need to do extra operations here to check s.rdd.
    // (For now, we just hardcode the join strategy.)
    import de.tuberlin.dima.matryoshka.util.Util.LBLSJoinStrategy._
    decideLBLSJoinStrategy(s) match {
      case Broadcast => rdd.broadcastJoinLeftToRightForeignKey(s.rdd)
      case Repartition => rdd join s.rdd
    }
  }

  def replicateCached[C: ClassTag](br: Broadcast[Map[L,C]]): LiftedRDD[L,(T,C)] = {
    rdd.map{case (k,v) => (k, (v, br.value(k)))}
  }

  def withClosure[C: ClassTag](closure: LiftedScalar[L,C]): LiftedRDD[L,(T,C)] = {
    replicate(closure)
  }

  def withClosure[C: ClassTag](closure: LiftedRDD[L,C]): LiftedRDD[L,(T,Iterable[C])] = {
    //rdd join closure.rdd.groupByKey()
    (rdd cogroup closure.rdd).flatMapValues{case (xs, ys) => xs.map(x => (x, ys))}
  }

  def withClosureCached[C: ClassTag](closureBr: Broadcast[Map[L,C]]): LiftedRDD[L,(T,C)] = {
    replicateCached(closureBr)
  }

  def coalesce(numPartitions: Int, shuffle: Boolean = false,
               partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
              (implicit ord: Ordering[(L,T)] = null): LiftedRDD[L,T] = {
    rdd.coalesce(numPartitions, shuffle, partitionCoalescer)(ord)
  }

  def force(): Unit = {
    rdd.force()
  }

  def dPersist(): LiftedRDD[L,T] = {
    rdd.defaultPersist()
  }
}

object LiftedRDD {

  import scala.language.implicitConversions
  private[lifting] implicit def rddToLiftedRDD[L: ClassTag, T: ClassTag](rdd: RDD[(L,T)])(implicit lc: LiftingContext[L]): LiftedRDD[L,T] = new LiftedRDD[L,T](rdd)

  implicit class LiftedPairRDDExtensions[L: ClassTag, K: ClassTag, V: ClassTag](lrdd: LiftedRDD[L, (K, V)]) {

    implicit val ilc: LiftingContext[L] = lrdd.liftingContext

    private[lifting] def pullInnerKey: RDD[((L, K), V)] = {
      lrdd.rdd.map{case (l, (k, v)) => ((l, k), v)}
    }

    def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
                                                  combOp: (U, U) => U): LiftedRDD[L, (K, U)] = {
      pullInnerKey
        .aggregateByKey(zeroValue)(seqOp, combOp)
        .pushInnerKey
    }

    def aggregateByKeyNoMapSideCombine[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
                                                    combOp: (U, U) => U): LiftedRDD[L, (K, U)] = {

      // Copy-paste from Spark's aggregateByKey-to-combineByKey bridge
      val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
      val zeroArray = new Array[Byte](zeroBuffer.limit)
      zeroBuffer.get(zeroArray)
      lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
      val createZero = () => cachedSerializer.deserialize[U](ByteBuffer.wrap(zeroArray))

      pullInnerKey
        .combineByKey((v: V) => seqOp(createZero(), v), seqOp, combOp, defaultPartitioner(lrdd.rdd), false)
        .pushInnerKey
    }

    def reduceByKey(func: (V, V) => V, mapSideCombine: Boolean = true): LiftedRDD[L, (K, V)] = {
//      pullInnerKey
//        .reduceByKey(func)
//        .pushInnerKey

      val rekeyed = pullInnerKey

      rekeyed
        .combineByKey(x => x, func, func, defaultPartitioner(rekeyed), mapSideCombine)
        .pushInnerKey
    }

    def join[W: ClassTag](other: LiftedRDD[L, (K, W)]): LiftedRDD[L, (K, (V, W))] = {
      val joined = pullInnerKey join other.pullInnerKey
      joined.pushInnerKey
    }

    // Use prepareAndPersistForJoin outside the loop to create the argument
    def joinWithCached[W: ClassTag](otherRekeyedAndRepartitionedAndCached: RDD[((L, K), W)]): LiftedRDD[L, (K, (V, W))] = {
      val joined = pullInnerKey join otherRekeyedAndRepartitionedAndCached
      joined.pushInnerKey
    }

    // Use prepareAndPersistForJoin outside the loop to create the argument
    def joinWithNonLiftedAndCached[W: ClassTag](otherRepartitionedAndCached: RDD[(K, W)]): LiftedRDD[L, (K, (V, W))] = {
      val rekeyed = lrdd.rdd.map{case (l,(k,v)) => (k,(l,v))} // this is not pullInnerKey
      val joined = rekeyed join otherRepartitionedAndCached
      joined.map{case (k, ((l, v), w)) => (l, (k, (v, w)))} // not pushInnerKey
    }

    def prepareAndPersistForJoin(): RDD[((L, K), V)] = {
      pullInnerKey.prepareAndPersistForJoin()
    }

    def leftAntiJoin[W: ClassTag](other: LiftedRDD[L, (K, W)]): LiftedRDD[L, (K,V)] = {
      (pullInnerKey leftAntiJoin other.pullInnerKey)
        .pushInnerKey
    }
  }

  implicit class LiftedDoubleRDDExtensions[L: ClassTag](lrdd: LiftedRDD[L, Double]) {

    implicit val ilc: LiftingContext[L] = lrdd.liftingContext

    def sum: LiftedScalar[L, Double] = {
      lrdd.rdd.reduceByKey(_+_, ilc.defaultLiftedScalarParallelism)
    }

    def average(implicit loopContext: LoopContext[L] = null): LiftedScalar[L, Double] = {
      lrdd.dPersist()
      //lrdd.force() // I tried this locally, didn't give a measurable speedup
      if (loopContext != null) loopContext.registerForUnpersist(lrdd)
      LiftedScalar.binaryScalarOp(lrdd.sum, lrdd.countAssumeNonEmpty())((sum, count) => sum / count.toDouble)
    }
  }

  def doWhile1[L: ClassTag, T: ClassTag]
        (in: LiftedRDD[L,T],
         body: (LoopContext[L], LiftedRDD[L,T]) => (LiftedRDD[L,T],LiftedScalar[L,Boolean])):
      LiftedRDD[L, T] = {
    implicit val ilc: LiftingContext[L] = in.liftingContext
    doWhile[L](Seq(in.rdd.castValuesTo[Any]), (loopContext: LoopContext[L], bodyIns: Seq[RDD[(L, Any)]]) => {
      relAssert(bodyIns.length == 1)
      val (bodyOut, cond) = body(loopContext, bodyIns.head.castValuesTo[T])
      (Seq(bodyOut.rdd.castValuesTo[Any]), cond)
    })(ilc).head.castValuesTo[T]
  }

  implicit class TwiceLiftedPairRDDExtensions[L1: ClassTag, L2: ClassTag, K: ClassTag, V: ClassTag](llrdd: LiftedRDD[(L1, L2), (K, V)]) {
    def joinWithLessLiftedAndCached[W: ClassTag](otherRekeyedAndRepartitionedAndCached: RDD[((L1, K), W)]): LiftedRDD[(L1, L2), (K, (V, W))] = {
      val rekeyed = llrdd.rdd.map{case ((l1, l2), (k, v)) => ((l1, k), (l2, v))}
      val joined = rekeyed join otherRekeyedAndRepartitionedAndCached
      implicit val ilc: LiftingContext[(L1, L2)] = llrdd.ilc
      new LiftedRDD(joined.map{case ((l1, k), ((l2, v), w)) => ((l1, l2), (k, (v, w)))})
    }
  }
}