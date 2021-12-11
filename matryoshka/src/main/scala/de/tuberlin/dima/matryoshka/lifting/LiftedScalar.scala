package de.tuberlin.dima.matryoshka.lifting

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import de.tuberlin.dima.matryoshka.util.Util._

import scala.reflect.ClassTag

// Called InnerScalar in the paper
// L is a primary key
class LiftedScalar[L: ClassTag, T: ClassTag](val rdd: RDD[(L,T)])(implicit val liftingContext: LiftingContext[L]) {

  val ilc: LiftingContext[L] = liftingContext

  def unaryOp[U: ClassTag](f: T => U): LiftedScalar[L,U] = {
    checkNumParts()
    rdd.mapValues(f)
  }

  // (forces)
  def prepareForBroadcast(): Broadcast[Map[L,T]] = {
    rdd.sparkContext.broadcast(rdd.collectAsMap().toMap)
  }

  def force(): Unit = {
    rdd.force()
  }

  def defaultPersist(): LiftedScalar[L,T] = {
    rdd.defaultPersist()
  }

  // Lifted equivalent of sc.parallelize(Seq(this))
  def toSingletonRDD: LiftedRDD[L,T] = {
    rdd.repartition(rdd.sparkContext.defaultParallelism)
  }

  // The following operations are private[lifting] because they are not lifted versions of actual scalar operations

  private[lifting] def filterValues(f: T => Boolean): LiftedScalar[L,T] = {
    rdd.filterValues(f)
  }

  // Would need to take care of partition count if uncommented
//  private[lifting] def union(other: LiftedScalar[L,T]): LiftedScalar[L,T] = {
//    rdd union other.rdd
//  }

  private[lifting] def merge[U: ClassTag](other: LiftedScalar[L,U]): LiftedScalar[L,(T,U)] = {
    // We just need to do the following:  this.rdd.join(other.rdd)
    // But we do a coGroup so that we can do consistency checks.
    // Note that RDD.join is implemented with a coGroup anyway, so there is no performance penalty.
    this.rdd.cogroup(other.rdd, numPartitions = liftingContext.defaultLiftedScalarParallelism).flatMapValues{case (it1, it2) =>
      val it1Size = it1.size
      val it2Size = it2.size
      if (it1Size != 1 || it2Size != 1) {
        // Hm, actually, it is ok if this does not hold in some cases:
        //  - where one of the merge inputs is in a doWhile, and the other comes from outside.
        //  - where one of the inputs had a filterOnOuter
        // So I might comment this out later when I cannot avoid such a merge call.
        throw new RuntimeException(s"Bug: LiftedScalar.merge found non-matching lift ID sets.\nit1: $it1,\nit2: $it2,")
      }
      Seq((it1.head, it2.head))
    }
  }

  private[lifting] def unliftToRDD: RDD[T] = {
    this.rdd.values
  }

  def checkNumParts(): Unit = {
    val ac = autoCoalesceEnabled(rdd.sparkContext)
    val numParts = rdd.getNumPartitions
    val default = liftingContext.defaultLiftedScalarParallelism
    relAssert(
      !ac ||
        numParts == default,
      s"ac: $ac, numParts: $numParts, default: $default")
  }
}

object LiftedScalar {

  import scala.language.implicitConversions
  private[lifting] implicit def rddToLiftedScalar[L: ClassTag, T: ClassTag](rdd: RDD[(L,T)])(implicit lc: LiftingContext[L]): LiftedScalar[L,T] = new LiftedScalar[L,T](rdd)

  def doWhile1[L: ClassTag, T: ClassTag]
        (in: LiftedScalar[L,T],
         body: (LoopContext[L], LiftedScalar[L,T]) => (LiftedScalar[L,T],LiftedScalar[L,Boolean])): LiftedScalar[L, T] = {
    implicit val ilc: LiftingContext[L] = in.liftingContext
    doWhile[L](Seq(in.rdd.castValuesTo[Any]), (loopContext: LoopContext[L], bodyIns: Seq[RDD[(L, Any)]]) => {
      relAssert(bodyIns.length == 1)
      val (bodyOut, cond) = body(loopContext, bodyIns.head.castValuesTo[T])
      (Seq(bodyOut.rdd.castValuesTo[Any]), cond)
    })(ilc).head.castValuesTo[T]
  }

  def doWhile2[L: ClassTag, T1: ClassTag, T2: ClassTag]
        (in: (LiftedScalar[L,T1], LiftedScalar[L,T2]),
         body: (LoopContext[L], LiftedScalar[L,T1], LiftedScalar[L,T2]) => (LiftedScalar[L,T1], LiftedScalar[L,T2], LiftedScalar[L,Boolean])): (LiftedScalar[L,T1], LiftedScalar[L,T2]) = {
    implicit val ilc: LiftingContext[L] = in._1.liftingContext
    val rseq = doWhile[L](Seq(in._1.rdd.castValuesTo[Any], in._2.rdd.castValuesTo[Any]), (loopContext: LoopContext[L], bodyIns: Seq[RDD[(L, Any)]]) => {
      relAssert(bodyIns.length == 2)
      val (bodyOut1, bodyOut2, cond) = body(loopContext, bodyIns.head.castValuesTo[T1], bodyIns.tail.head.castValuesTo[T2])
      (Seq(bodyOut1.rdd.castValuesTo[Any], bodyOut2.rdd.castValuesTo[Any]), cond)
    })(ilc)
    (rseq.head.castValuesTo[T1], rseq.tail.head.castValuesTo[T2])
  }

  def binaryScalarOp[L: ClassTag, T1: ClassTag, T2: ClassTag, R: ClassTag](in1: LiftedScalar[L,T1], in2: LiftedScalar[L,T2])(op: (T1, T2) => R): LiftedScalar[L,R] = {
    (in1 merge in2).unaryOp(x => op(x._1, x._2))
  }

  def ternaryScalarOp[L: ClassTag, T1: ClassTag, T2: ClassTag, T3: ClassTag, R: ClassTag](in1: LiftedScalar[L,T1], in2: LiftedScalar[L,T2], in3: LiftedScalar[L,T3])(op: (T1, T2, T3) => R): LiftedScalar[L,R] = {
    (in1 merge in2 merge in3).unaryOp{case ((x, y), z) => op(x, y, z)}
  }

  implicit class TwiceLiftedScalarExtensions[L1: ClassTag, L2: ClassTag, T: ClassTag](lls: LiftedScalar[(L1, L2), T]) {
    private[lifting] def unliftToLiftedRDD: LiftedRDD[L1, T] = {
      implicit val outerLC: LiftingContext[L1] = lls.liftingContext.outerLiftingContext.asInstanceOf[LiftingContext[L1]]
      lls.rdd.map {case ((l1, l2), t) => (l1, t)}
    }
  }
}