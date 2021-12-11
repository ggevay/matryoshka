package de.tuberlin.dima.matryoshka.kmeans

import de.tuberlin.dima.matryoshka.lifting.LiftedScalar
import de.tuberlin.dima.matryoshka.lifting.{LiftedRDD, LiftedScalar}
import org.apache.spark
import org.apache.spark._
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd._

import scala.util.Random

object KMeansLev12 {

  import de.tuberlin.dima.matryoshka.util.Util._
  import de.tuberlin.dima.matryoshka.lifting._
  import de.tuberlin.dima.matryoshka.lifting.LiftedScalar._
  import Util._

  object WithClosureStrategy extends Enumeration {
    type WithClosureStrategy = Value
    val BroadcastLeft, BroadcastRight, CartesianRDD, Optimizer = Value
  }
  import WithClosureStrategy._

  def main(args: Array[String]): Unit = {

    // --- Parse args

    val numPoints = args(0).toLong
    val K         = args(1).toInt
    val dim       = args(2).toInt
    val runs      = args(3).toInt

    val withClosureStrategy =
      if (args.length == 4)
        Optimizer
      else WithClosureStrategy.withName(args(4))

    val eps = 1

    // --- Spark setup

    implicit val sc: SparkContext = sparkSetup(this)

    // --- Generate input

    val points = getKMeansRandomInput(numPoints, dim).defaultPersist()

    // --- KMeans

    val t0 = System.nanoTime()

    val runsRDD = sc.range(0, runs)

    val result = runsRDD.mapWithLiftedUDF((run: LiftedScalar[Long, Long]) => {
      implicit val liftingContext: LiftingContext[Long] = run.liftingContext

      val initMeans = run.unaryOp(run => {
        val rnd = new Random(run)
        val initMeans = spark.Util.createCompactBuffer(Seq.fill(K)(randomPoint(rnd, dim))).asInstanceOf[Seq[DenseVector]]
        initMeans
      })

      //val initSumDistance = liftingContext.conjure(Double.PositiveInfinity)

      val initIt = liftingContext.conjure(0)

      //val (finalMeans, finalSumDistance) = doWhile2((initMeans, initSumDistance), (loopContext, means: LiftedScalar[Long, Seq[DenseVector]], sumDistance: LiftedScalar[Long, Double]) => {
      val (finalMeans, finalSumDistance) = doWhile2((initMeans, initIt), (loopContext: LoopContext[Long], means: LiftedScalar[Long, Seq[DenseVector]], it: LiftedScalar[Long, Int]) => {

        // Hack the broadcasting for now.
        // (Doing it properly would be a half-lifted withClosure)
        // And on the issue of whether to broadcast it or cross:
        //   - If means is large (number of partitions), then it has to be a cross (we will need an if for this in the proper implementation).
        //   - If means is small (1 partition), then it would seem at first glance that a cross is still fine. However,
        //     it's better to do it with a broadcast, because that has a more efficient implementation for getting that one partition to all machines.
        //     (And the loop order is not an issue if we do a mapPartitions instead of a map)
        //val assignment = new LiftedRDD[Long,(DenseVector, DenseVector)](points.cartesian(means.rdd).map {case (point, (liftID, means)) =>
//        val assignment = new LiftedRDD[Long,(DenseVector, DenseVector)](means.rdd.cartesian(points).map {case ((liftID, means), point) => // means should be left input to cross
//          (liftID, (means.minBy(m => Vectors.sqdist(point, m)), point))
//        })

        val crossed = withClosureStrategy match {
          case BroadcastLeft =>
            means.rdd.cartesianBroadcastLeft(points, loopContext)
          case BroadcastRight =>
            means.rdd.repartition(sc.defaultParallelism).cartesianBroadcastRight(points, loopContext)
          case CartesianRDD =>
            means.defaultPersist()
            loopContext.registerForUnpersist(means)
            means.rdd.cartesian(points)
          case Optimizer =>
            // We do the if based on liftedScalarElemCount instead of means.rdd.getNumPartitions in order to avoid being affected by whether autoCoalesce is enabled
            if (means.liftingContext.liftedScalarElemCount <= autoCoalescePartitionSize) {
              relAssert(!autoCoalesceEnabled(sc) || means.rdd.getNumPartitions == 1, s"autoCoalesceEnabled: ${autoCoalesceEnabled(sc)}, means.rdd.getNumPartitions: ${means.rdd.getNumPartitions}")
              println(s"numPartLeft == 1")
              means.rdd.cartesianBroadcastLeft(points, loopContext) // the common case
            } else {
              relAssert(!autoCoalesceEnabled(sc) || means.rdd.getNumPartitions > 1)
              means.defaultPersist()
              loopContext.registerForUnpersist(means)
              val leftSize = means.rdd.estimateSize()
              val rightSize = points.estimateSize()
              println(s"leftSize: $leftSize, rightSize: $rightSize")
              if (leftSize < rightSize)
                means.rdd.cartesianBroadcastLeft(points, loopContext)
              else
                means.rdd.repartition(sc.defaultParallelism).cartesianBroadcastRight(points, loopContext)
            }
        }

        val assignment = new LiftedRDD[Long,(DenseVector, DenseVector)](crossed.map {case ((liftID, means), point) =>
          (liftID, (means.minBy(m => Vectors.sqdist(point, m)), point))
        })

        assignment.defaultPersist()
        loopContext.registerForUnpersist(assignment.rdd)

        val newMeans = assignment.aggregateByKeyNoMapSideCombine((Vectors.zeros(dim).asInstanceOf[DenseVector], 0))(
          (sc, v) => (add(sc._1, v), sc._2 + 1),
          (sc1, sc2) => (add(sc1._1, sc2._1), sc1._2 + sc2._2)
        )
          .map{case (_, (s, c)) => Vectors.dense(s.toArray.map(d => d/c)).asInstanceOf[DenseVector]}
          .collect

        val newSumDistance = assignment.map{case (a,b) => Vectors.sqdist(a,b)}.sum

        newSumDistance.force() // !!! This should be here only if the cond is numSteps and does not depend on newSumDistance

//        val cond = binaryScalarOp(sumDistance, newSumDistance)(
//          (sumDistance, newSumDistance) => Math.abs(sumDistance - newSumDistance) > eps)

        val newIt = it.unaryOp(_ + 1)
        newIt.defaultPersist()
        loopContext.registerForUnpersist(newIt.rdd)
        val cond = newIt.unaryOp(_ <= numSteps)

        //(newMeans, newSumDistance, cond)
        (newMeans, newIt, cond)
      })

      finalSumDistance
    }).min()

    println(s"Final sumDistance: $result")

    val t1 = System.nanoTime()
    println("non-warmup-time: " + round2((t1 - t0).asInstanceOf[Double]/1000000000))

  }
}
