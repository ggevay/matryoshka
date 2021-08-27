package de.tuberlin.dima.matryoshka.util

import de.tuberlin.dima.matryoshka.lifting._
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

import scala.reflect.ClassTag
import scala.util.Random

object Util {

  val local = true

  def sparkSetup(caller: Any): SparkContext = {
    val conf = new SparkConf().setAppName(caller.getClass.getSimpleName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    if (local) {
      conf.setMaster("local[10]")
      conf.set("spark.local.dir", "TOBEFILLED")
    }
    conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    conf.registerKryoClasses(Array(classOf[DenseVector]))
    implicit val sc: SparkContext = new SparkContext(conf)

    if (local)
      sc.setCheckpointDir("TOBEFILLED")
    else
      sc.setCheckpointDir("hdfs://TOBEFILLED")

    println("=== sc.defaultParallelism: " + sc.defaultParallelism)

    sc
  }

  def round2(d: Double): Double = {
    Math.round(d*100.0)/100.0
  }

  def relAssert(assertion: Boolean, msg: String = ""): Unit = {
    if (!assertion)
      throw new java.lang.AssertionError("assertion failed: " + msg)
  }

  class StopWatch {

    private val t0 = System.nanoTime()

    def done(): Unit = {
      val t1 = System.nanoTime()
      println("non-warmup-time: " + round2((t1 - t0).asInstanceOf[Double]/1000000000))
    }

    def take(): Long = {
      System.nanoTime() - t0
    }
  }

  def randomRDD[A: ClassTag](seed: Long, size: Long, filler: Random => A)(implicit sc: SparkContext): RDD[A] = {
    val para = sc.defaultParallelism
    sc.range(0, para)
      .flatMap( ind => {
        val rnd = new Random(seed + ind)

        relAssert(ind < para)
        relAssert(size / para + 1 <= Int.MaxValue)
        val partSize =
          if (ind < size % para)
            size / para + 1
          else
            size / para

        (1 to partSize.asInstanceOf[Int])
          .view // to make it lazy!
          .map(_ => filler(rnd))
          .iterator
      })
  }

  def randomRDDWithFillerFactory[A: ClassTag](seed: Long, size: Long, fillerFactory: Long => () => A)(implicit sc: SparkContext): RDD[A] = {
    val para = sc.defaultParallelism
    sc.range(0, para)
      .flatMap( ind => {
        val filler = fillerFactory(seed + ind)

        relAssert(ind < para)
        relAssert(size / para + 1 <= Int.MaxValue)
        val partSize =
          if (ind < size % para)
            size / para + 1
          else
            size / para

        (1 to partSize.asInstanceOf[Int])
          .view // to make it lazy!
          .map(_ => filler())
          .iterator
      })
  }

  // warm up the JVM
  def warmup[A: ClassTag](r: RDD[A]): RDD[A] = {
    r.count()

    r.count()

    r.cache()
    r.count()
    r.unpersist(blocking = true)

    r.cache()
    r.count()
    r.unpersist(blocking = true)

    val r2 = r.map(x=>x)
    r2.cache()
    r2.checkpoint()
    r2.count()
    r2.unpersist(blocking = true)

    r2
  }

  implicit class SeqExtensions[T](val in: Seq[T]) {

    // This already exists
//    def distinct(): Seq[T] = {
//      in.groupBy(x=>x).keys.toSeq
//    }

    def count(): Int = in.size

    def average()(implicit num: Numeric[T]): Double = {
      num.toDouble(in.sum) / in.count().toDouble
    }

    def print(pref: String = ">> "): Unit = {
      println(pref + in.mkString(", "))
    }
  }

  implicit class PairSeqExtensions[K,V](val in: Seq[(K,V)]) {

    def aggregateByKey[U](zeroValue: U)(seqOp: (U, V) => U,
                                        combOp: (U, U) => U): Seq[(K, U)] = {
      in.groupBy(_._1).mapValues(grp => grp.map(_._2).foldLeft(zeroValue)(seqOp)).toSeq
    }

    def reduceByKey(func: (V, V) => V): Seq[(K, V)] = {
      in.groupBy(_._1).mapValues(grp => grp.map(_._2).reduce(func)).toSeq
    }

    def join[W](other: Seq[(K, W)]): Seq[(K, (V, W))] = {
      val grouped = other.groupBy(_._1)
      in.flatMap{case (k,v) => grouped.getOrElse[Seq[(K,W)]](k, Seq()).map(o => (k,(v,o._2)))}
    }

    def keys: Seq[K] = in.map(_._1)

    def leftAntiJoin[W](other: Seq[(K, W)]): Seq[(K,V)] = {
      val set = other.keys.toSet
      in.filter{case (k,_) => !set.contains(k)}
    }
  }

  val autoCoalescePartitionSize = 1000

  val autoCoalesceEnabled: SparkContext => Boolean =
    (sc: SparkContext) => sc.getConf.getBoolean("spark.nesting.autoCoalesce", defaultValue = true) // It has to have "spark.", otherwise spark-submit filters it out

  implicit class RDDExtensions1[T: ClassTag](val in: RDD[T]) extends Serializable {

    def minBy[B](f: T => B)(implicit cmp: Ordering[B]): T = {
      in.min()(Ordering.by(f))
    }

    def print(pref: String = ">> "): Unit = {
      val arr = in.collect()
      println(pref + arr.mkString(", "))
    }

    def printPartSizes(): Unit = {
      val partSizes = in.mapPartitions(it=>Seq(it.size).toIterator).collect()
      println("partSizes: " + partSizes.mkString(", "))
    }

    def force(): in.type = {
      //in.foreach(_) // not good
      in.foreach(_ => ())
      in
    }

    def defaultPersist(): RDD[T] = {
      in.persist(defaultRDDPersistenceLevel)
    }

    def average()(implicit num: Numeric[T]): Double = {
      relAssert(in.getStorageLevel == StorageLevel.NONE) // We have this assert because it unpersists at the end
      in.defaultPersist()
      val res = in.sum / in.count().toDouble
      in.unpersist()
      res
    }

    def autoCoalesceAndPersist(): RDD[T] = {
      if (autoCoalesceEnabled(in.sparkContext)) {
        println("autoCoalesce enabled")
        in.defaultPersist()
        val c = in.count()
        val p = Math.min(in.getNumPartitions, Math.max(1, (c.toDouble / autoCoalescePartitionSize.toDouble).ceil.toLong)).toInt
        relAssert(!(c <= autoCoalescePartitionSize && p > 1 || p > c / autoCoalescePartitionSize + 1))
        val res = in.coalesce(p, shuffle = true) // see scaladoc of coalesce
        res.defaultPersist()
        res.force()
        in.unpersist()
        res
      } else {
        println("autoCoalesce disabled")
        in.defaultPersist()
        in.force()
      }
    }

    def cartesianBroadcastLeft[U: ClassTag, L: ClassTag](right: RDD[U], loopContext: LoopContext[L]): RDD[(T, U)] = {
      println("cartesianBroadcastLeft")
      val sc = in.sparkContext
      val left = in
      val brLeft = sc.broadcast(left.collect())
      loopContext.registerForUnpersist(brLeft)
      right.mapPartitions(rightIt => {
        val rightArr = rightIt.toArray
        val crossedArr = for(x <- brLeft.value; y <- rightArr) yield (x,y)
        crossedArr.toIterator
      })
    }

    def cartesianBroadcastRight[U: ClassTag, L: ClassTag](right: RDD[U], loopContext: LoopContext[L]): RDD[(T, U)] = {
      println("cartesianBroadcastRight")
      val sc = in.sparkContext
      val left = in
      val brRight = sc.broadcast(right.collect())
      loopContext.registerForUnpersist(brRight)
      left.mapPartitions(leftIt => {
        val leftArr = leftIt.toArray
        val crossedArr = for(x <- leftArr; y <- brRight.value) yield (x,y)
        crossedArr.toIterator
      })
    }

    def estimateSize(): Long = {
      in.mapPartitions(it => {
        Iterator.single(SizeEstimator.estimate(it.toArray)) // The toArray is needed to force the iterator
      }).reduce(_+_)
    }

    def estimateSizeBySampling(): Long = {
      val frac = 0.1
      (in.sample(withReplacement = false, frac).estimateSize().toDouble / frac).round
    }
  }

  val defaultRDDPersistenceLevel: StorageLevel = StorageLevels.MEMORY_AND_DISK_SER

  implicit class PairRDDExtensions2[K: ClassTag, V: ClassTag](rdd: RDD[(K,V)]) {

    def prepareAndPersistForJoin(): RDD[(K,V)] = {
      rdd
        .partitionBy(Partitioner.defaultPartitioner(rdd))
        .defaultPersist()
    }

    def reduceByKeyNoCombiner(func: (V, V) => V): RDD[(K, V)] = {
      rdd.combineByKey(x => x, func, func, defaultPartitioner(rdd), mapSideCombine = false)
    }

    def leftAntiJoin[W: ClassTag](other: RDD[(K, W)]): RDD[(K,V)] = {
      rdd.cogroup(other).flatMap {case (key, (it1, it2)) =>
        if (it2.isEmpty)
          it1.map(x => (key, x))
        else
          Seq.empty
      }
    }

    // Join, with right broadcasted. Assumes that left has a foreign key to right, i.e.,
    //  - K is primary key in right (fails silently if this is not the case), and
    //  - right should have a match for each key in left (fails noisily if this is not the case)
    def broadcastJoinLeftToRightForeignKey[W: ClassTag](other: RDD[(K,W)]): RDD[(K,(V,W))] = {
      val br = rdd.sparkContext.broadcast(other.collectAsMap())
      rdd.map{case (k,v) => (k, (v, br.value(k)))}
    }

    // Sames as above, but with an already collected map
    def broadcastJoinLeftToRightForeignKey[W: ClassTag](br: Broadcast[scala.collection.Map[K,W]]): RDD[(K,(V,W))] = {
      rdd.map{case (k,v) => (k, (v, br.value(k)))}
    }

    def castValuesTo[T]: RDD[(K, T)] = rdd.mapValues(x => x.asInstanceOf[T])
  }

  var manualGCTime = 0d
  def GCOnAllExecutors(sc: SparkContext): Unit = {
    val stopWatch = new StopWatch

    sc.range(0, sc.defaultParallelism).foreach(_ => System.gc())

    manualGCTime += stopWatch.take()
  }

  def printManualGCTime(): Unit = {
    println("manualGCTime: " + manualGCTime)
  }

  implicit class LongExtensions(val x: Long) {
    def toIntOrThrow: Integer = {
      if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE)
        throw new RuntimeException
      x.toInt
    }
  }

  implicit class StringExtensions(val s: String) {
    def isDouble: Boolean = {
      try {
        s.toDouble
      } catch {
        case e: NumberFormatException => return false
      }
      true
    }
  }

  object LBLSJoinStrategy extends Enumeration {
    type LBLSJoinStrategy = Value
    val Broadcast, Repartition, Optimizer = Value
  }
  import LBLSJoinStrategy._

  var LBLSJoinStrategyConfig: LBLSJoinStrategy = Optimizer

  def decideLBLSJoinStrategy(ls: LiftedScalar[_,_]): LBLSJoinStrategy = {
    LBLSJoinStrategyConfig match {
      case Optimizer =>
        if (ls.liftingContext.liftedScalarElemCount < ls.rdd.sparkContext.defaultParallelism) {
          println(s"decideLBLSJoinStrategy decided Broadcast (${ls.liftingContext.liftedScalarElemCount} < ${ls.rdd.sparkContext.defaultParallelism})")
          Broadcast
        } else {
          println(s"decideLBLSJoinStrategy decided Repartition (${ls.liftingContext.liftedScalarElemCount} >= ${ls.rdd.sparkContext.defaultParallelism})")
          Repartition
        }
      case cfg =>
        println(s"decideLBLSJoinStrategy took ${cfg} from config")
        cfg
    }
  }
}
