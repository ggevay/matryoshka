package de.tuberlin.dima.matryoshka.lifting

import scala.reflect.ClassTag

object ControlFlowLiftingOld {

  import org.apache.spark._
  import org.apache.spark.rdd._

  def doWhile[A](init: A, body: A => (A, Boolean)): A = {
    var bodyIn = init
    var more = true
    do {
      val bodyOut_more = body(bodyIn)
      bodyIn = bodyOut_more._1
      more = bodyOut_more._2
    } while (more)
    bodyIn
  }


  def main(args: Array[String]): Unit = {

    // test-only version
    def oldLiftedDoWhile[A: ClassTag](initS: RDD[A], body: A => (A, Boolean)): RDD[A] = {
      var bodyInS = initS
      var more = true
      var bodyOutS_moreS: RDD[(A, Boolean)] = null
      var res = initS.sparkContext.emptyRDD[A]
      do {
        bodyOutS_moreS = bodyInS.map(body).cache()
        bodyInS = bodyOutS_moreS.filter(x => x._2).map(x => x._1)
          .cache()
        res = res union bodyOutS_moreS.filter(x => !x._2).map(x => x._1)
        res.cache()
        //more = bodyOutS_moreS.map(x => x._2).reduce(_ || _)
        more = !bodyInS.isEmpty()
      } while (more)
      res
    }

    val conf = new SparkConf().setAppName("NestedMicrobenchmark1")
      .setMaster("local[10]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    println("=== sc.defaultParallelism: " + sc.defaultParallelism)
    val para = sc.defaultParallelism


    val xsInit = sc.range(1, 4)

    // Orig:
    //val res = xsInit.map(x => doWhile(x, (x: Long) => (x + 1, x < 5)))

    // Lifted:
    val res = oldLiftedDoWhile(xsInit, (x: Long) => (x + 1, x < 5))


    val collected = res.collect()
    sc.stop()
    collected.foreach(println)






  }

}
