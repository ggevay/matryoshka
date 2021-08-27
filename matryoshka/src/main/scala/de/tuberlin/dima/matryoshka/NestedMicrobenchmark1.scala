package de.tuberlin.dima.matryoshka

import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object NestedMicrobenchmark1 {

  import org.apache.spark.RDDSplittingSerialized._
  //import RDDSplitting._

  def main(args: Array[String]): Unit = {

//    // Lazyness test
//    // Put a breakpoint in each of the map functions, and hit F9 repeatedly.
//    // With the view, we alternate between the two map functions,
//    // but without the view the first one gets hit first for all the elements in the range before the second one gets any hits.
//    val aaa = (1 to 10)
//      .view
//      .map(x => {
//        -x
//      }).map(x => {
//        println(x)
//      })
//    aaa.foreach(println)
//    return


    val million = 1000*1000
    val total = args(0).toLong * million //80 million
    val groupSize = args(1).toLong * million // 5 million
    val range = (total / groupSize).toInt
    assert(total / groupSize <= Int.MaxValue)
    //assert(total % groupSize == 0)
    if (total % groupSize != 0) {
      println("Warning: total % groupSize != 0")
    }


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      //.setMaster("local[10]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    println("=== sc.defaultParallelism: " + sc.defaultParallelism)
    val para = sc.defaultParallelism


//    val xxx = groupByToRDDs(sc.range(0, 10))(x => x).map(r => (r._1, r._2.collect))
//    xxx
//      .map(karr => (karr._1, karr._2.mkString(",")))
//      .foreach(x => println(x))
//    return


    val xys = sc.range(0, para)
      .flatMap( seed => {
        val rnd = new Random(seed)

        assert(seed < para)
        assert(total / para + 1 <= Int.MaxValue)
        val numRecords =
          if (seed < total % para)
            total / para + 1
          else
            total / para

        (1 to numRecords.asInstanceOf[Int])
          .view // to make it lazy!
          .map{_ =>
            (rnd.nextInt(range).asInstanceOf[Long], rnd.nextInt(range).asInstanceOf[Long])
            // ~40 byte, so 1G should hold 25 million
            // locally, with Xmx4g, it fails with groupSize=8*million (total=80*million)
            //   fine with groupSize=5*million
          }.iterator
      })

    //xys.collect().foreach(println)

    val t0 = System.nanoTime()

//    //=== Outer Parallel
//    val maxCounts = xys.groupBy(xy => xy._1).map(g => {
//      //stringOf(g)
//      (g._1, g._2.groupBy(xy => xy._2).map(ig => (ig._1, ig._2.size)).maxBy(a => a._2))
//    })
//    maxCounts.collect().foreach(println)

//    // === Inner Parallel
//    val maxCounts = xys.groupByIntoRDDs(xy => xy._1).map(g => {
//      val switched = g._2.map(kv => (kv._2,kv._1))
//      val combined = switched.combineByKey(x=>1, (x: Int, y: Long)=>x+1, (x: Int, y: Int)=>x+y, defaultPartitioner(switched), true)
//      val maxed = combined
//        .map(kv => (kv._2,kv._1))
//        .max()
//      (g._1, (maxed._2, maxed._1))
//    })
//    maxCounts.foreach(println)

    // === Flat
    val max_2 = (yc1: (Long, Int), yc2: (Long, Int)) => if (yc1._2 > yc2._2) (yc1._1, yc1._2) else (yc2._1, yc2._2)
    val compos = xys.map(xy => (xy, ())) // composite key from both fields
    val maxCounts = compos
      .combineByKey(x=>1, (x: Int, _: Unit)=>x+1, (x: Int, y: Int)=>x+y, defaultPartitioner(compos), true)
      .map {case ((x,y), count) => (x, (y, count))}
      .combineByKey(yc => yc, max_2, max_2, defaultPartitioner(compos), true)
    maxCounts.collect().foreach(println)


    val t1 = System.nanoTime()
    println("--- Time: " + (t1 - t0).asInstanceOf[Double]/1000000000 + "s")
  }

}
