package de.tuberlin.dima.matryoshka

import org.apache.spark.CartesianRDDHackedLocality
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import de.tuberlin.dima.matryoshka.util.Util._
import de.tuberlin.dima.matryoshka.lifting.LiftedScalar._

import scala.collection.mutable

package object lifting {

  class LiftingContext[L: ClassTag](val base: RDD[L], val outerLiftingContext: LiftingContext[_] = null) extends Serializable {

    val defaultLiftedScalarParallelism: Int = base.getNumPartitions

    val liftedScalarElemCount: Long = base.count()

    def conjure[T: ClassTag](x: T): LiftedScalar[L,T] = {
      implicit val ilc: LiftingContext[L] = this
      base.map(l => (l,x))
    }
  }

  import scala.language.implicitConversions
  private[lifting] implicit def LiftingContextFromLiftedScalar[L: ClassTag, T: ClassTag](ls: LiftedScalar[L,T]): LiftingContext[L] = ls.liftingContext

  implicit class RDDExtensions[T: ClassTag](rdd: RDD[T]) {

    def mapWithLiftedUDF[U: ClassTag](liftedMapFunc: LiftedScalar[Long, T] => LiftedScalar[Long, U]): RDD[U] = {
      liftedMapFunc(rdd.liftToScalar).unliftToRDD
    }

    // This overload creates lift IDs by itself. (groupByIntoNestedRDD creates them by a UDF)
    private[lifting] def liftToScalar: LiftedScalar[Long, T] = {
      val rddWithLiftIDs = rdd.zipWithUniqueId().map {case (x,id) => (id,x)}.autoCoalesceAndPersist()
      implicit val ilc: LiftingContext[Long] = new LiftingContext(rddWithLiftIDs.map(_._1))
      new LiftedScalar(rddWithLiftIDs)
    }

    def hackedCross[U: ClassTag](other: RDD[U]): RDD[(T, U)] = {
      new CartesianRDDHackedLocality(rdd.sparkContext, rdd, other)
    }

    def groupByIntoNestedRDD[K: ClassTag](func: T => K): FlattenedRDD[K,K,T] = FlattenedRDD.fromGrouping(rdd, func)
  }

  implicit class PairRDDExtensions[K: ClassTag, V: ClassTag](rdd: RDD[(K,V)]) {

    def groupByKeyIntoNestedRDD(): FlattenedRDD[K,K,V] = FlattenedRDD.fromGrouping(rdd)

    def cogroupIntoNestedRDD[W: ClassTag](other: RDD[(K,W)]): FlattenedRDDBinary[K,K,V,W] = FlattenedRDDBinary.fromGrouping(rdd, other)

    def filterValues(f: V => Boolean): RDD[(K,V)] = {
      rdd.mapPartitions(iter => iter.filter { case (_, v) => f(v) }, preservesPartitioning = true)
    }
  }

  implicit class PulledInnerKeyLKVRDDExtensions[L: ClassTag, K: ClassTag, V: ClassTag](rdd: RDD[((L,K),V)]) {
    def pushInnerKey: RDD[(L,(K,V))] = {
      rdd.map{case ((l, k), v) => (l, (k, v))}
    }
  }

  class LoopContext[L: ClassTag](var base: RDD[L]) {

    private val rddsToUnpersist: ArrayBuffer[RDD[_]] = new ArrayBuffer
    private val brdsToUnpersist: ArrayBuffer[Broadcast[_]] = new ArrayBuffer

    // This function can be used in loop bodies to register RDDs for unpersisting later, after the body was forced.
    def registerForUnpersist(rdd: RDD[_]): Unit = {
      rddsToUnpersist += rdd
    }

    def registerForUnpersist(ls: LiftedScalar[_,_]): Unit = {
      rddsToUnpersist += ls.rdd
    }

    def registerForUnpersist(lrdd: LiftedRDD[_,_]): Unit = {
      rddsToUnpersist += lrdd.rdd
    }

    def registerForUnpersist(broadcast: Broadcast[_]): Unit = {
      brdsToUnpersist += broadcast
    }

    def unpersistAllRegistered(): Unit = {
      rddsToUnpersist.foreach(_.unpersist())
      brdsToUnpersist.foreach(_.unpersist())
      rddsToUnpersist.clear()
      brdsToUnpersist.clear()
    }
  }

  def doWhile[L: ClassTag](initBodyIns: Seq[RDD[(L, Any)]],
                           body: (LoopContext[L], Seq[RDD[(L, Any)]]) => (Seq[RDD[(L, Any)]], LiftedScalar[L, Boolean]))
                          (liftingContext: LiftingContext[L]): Seq[RDD[(L, Any)]] = { // liftingContext could be implicit, but there is an IntelliJ bug
    // TODO: double-check caching (check WebUI)
    val sc = initBodyIns.head.sparkContext
    var bodyIns = initBodyIns
    var moreAny = true
    var res = Seq.fill(initBodyIns.length)(sc.emptyRDD[(L, Any)])
    var bodyOutsWithCond = Seq.fill(initBodyIns.length)(sc.emptyRDD[(L,(Any, Boolean))])
    var oldBodyOutsWithCond: Seq[RDD[(L,(Any, Boolean))]] = null
    var it = 0
    val loopContext = new LoopContext[L](liftingContext.base)
    do {
      val (bodyOuts, cond) = body(loopContext, bodyIns)

      loopContext.base = cond.filterValues(x => x).rdd.keys

      oldBodyOutsWithCond = bodyOutsWithCond

      import de.tuberlin.dima.matryoshka.util.Util.LBLSJoinStrategy._
      var condMapBr: Broadcast[scala.collection.Map[L, Boolean]] = null
      var condColl: Array[(L, Boolean)] = null
      decideLBLSJoinStrategy(cond) match {
        case Broadcast =>
          condColl = cond.rdd.collect()
          val condMap = new mutable.HashMap[L, Boolean]
          condMap.sizeHint(condColl.length)
          condColl.foreach { pair => condMap.put(pair._1, pair._2) }
          condMapBr = sc.broadcast(condMap: scala.collection.Map[L, Boolean])
          bodyOutsWithCond = bodyOuts.map(bodyOut => bodyOut.broadcastJoinLeftToRightForeignKey[Boolean](condMapBr)
            .defaultPersist())
        case Repartition =>
          cond.dPersist()
          cond.checkNumParts()
          bodyOutsWithCond = bodyOuts.map(bodyOut => bodyOut.join(cond.rdd, bodyOut.getNumPartitions)
            .defaultPersist())
      }

      bodyIns = bodyOutsWithCond.map(_.filterValues(x => x._2).mapValues(x => x._1))
//      if (it % 5 == 0)
//        bodyIns.foreach(_.checkpoint())

      // There are a lot of tricky performance considerations here:
      // - We shouldn't force or checkpoint the UnionRDDs (not even at every 5 steps), because that would make us
      //   quadratic in the total number of elements.
      // - We don't want recomputation of old loop steps, so we want to checkpoint the constituent RDDs that make
      //   up the union.
      // - It's totally not enough to just cache the constituent RDDs, because:
      //   - They could fall out of the cache, and then trigger recomputations of old loop steps when forcing the
      //     union result.
      //   - They would pin the metadata of the main RDDs of old loop steps.
      //   - They would pin checkpoint data of the main RDDs of old loop steps. (I think now we keep only the latest
      //     of the checkpoints of the main RDDs.)
      // - We need to manually force the constituent RDDs to actually perform the checkpoints,
      //   because otherwise they would be forced only at the end, and there would be no checkpoints.
      // So to sum up the solution, we cleanly separate the union stuff from the rest of the loop by checkpointing
      // and forcing the constituent RDDs.
      val toAddToRes = bodyOutsWithCond.map(_.filterValues(x => !x._2).mapValues(x => x._1)
        .persist(StorageLevel.MEMORY_AND_DISK_SER) // Gonna be serialized anyway because of the localCheckpoint
        .localCheckpoint()
        .force()
      )
      // The dependency chains can come out from the body only through bodyOuts and cond.
      // Both were just forced through bodyOutsWithCond and then toAddToRes.
      loopContext.unpersistAllRegistered()
      oldBodyOutsWithCond.map(_.unpersist())
      if (condMapBr != null)
        condMapBr.unpersist()

      res = (res zip toAddToRes) map {case (a,b) => a union b}

      decideLBLSJoinStrategy(cond) match {
        case Broadcast =>
          moreAny = condColl.map(x => x._2).reduce(_ || _)
        case Repartition =>
          moreAny = cond.rdd.map(x => x._2).reduce(_ || _)
          cond.rdd.unpersist()
      }

      it = it + 1
      println(s"It: $it")
    } while (moreAny)
    bodyOutsWithCond.foreach(_.unpersist())
    // todo: unpersist all toAddToRes somehow.
    //  We could persist and force res, but then the question is where do we unpersist res...
    res
  }

  def doWhile3RddRddScalar[L: ClassTag, T1: ClassTag, T2: ClassTag, T3: ClassTag]
        (in1: LiftedRDD[L,T1], in2: LiftedRDD[L,T2], in3: LiftedScalar[L,T3],
         body: (LoopContext[L], LiftedRDD[L,T1], LiftedRDD[L,T2], LiftedScalar[L,T3]) => (LiftedRDD[L,T1], LiftedRDD[L,T2], LiftedScalar[L,T3], LiftedScalar[L, Boolean])):
      (LiftedRDD[L,T1], LiftedRDD[L,T2], LiftedScalar[L,T3]) = {
    implicit val ilc: LiftingContext[L] = in1.liftingContext
    val rseq = doWhile[L](Seq(in1.rdd.castValuesTo[Any], in2.rdd.castValuesTo[Any], in3.rdd.castValuesTo[Any]), (loopContext: LoopContext[L], bodyIns: Seq[RDD[(L, Any)]]) => {
      relAssert(bodyIns.length == 3)
      val bodyIns1 = bodyIns.head
      val bodyIns2 = bodyIns.tail.head
      val bodyIns3 = bodyIns.tail.tail.head
      val (bodyOut1, bodyOut2, bodyOut3, cond) = body(loopContext, bodyIns1.castValuesTo[T1], bodyIns2.castValuesTo[T2], bodyIns3.castValuesTo[T3])
      (Seq(bodyOut1.rdd.castValuesTo[Any], bodyOut2.rdd.castValuesTo[Any], bodyOut3.rdd.castValuesTo[Any]), cond)
    })(ilc)
    (rseq.head.castValuesTo[T1], rseq.tail.head.castValuesTo[T2], rseq.tail.tail.head.castValuesTo[T3])
  }
}
