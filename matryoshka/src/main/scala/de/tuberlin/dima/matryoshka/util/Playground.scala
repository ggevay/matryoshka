package de.tuberlin.dima.matryoshka.util

import shapeless._
import de.tuberlin.dima.matryoshka.util.Util.sparkSetup
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd._
import poly._
import Util._
import org.apache.spark.util.SizeEstimator

import scala.reflect.ClassTag

object Playground {

  def main(args: Array[String]): Unit = {


    //test1()

    //test2()

    //test3()

    //test4()

    //test5()

    //test6()

    test7()
  }

  def test7(): Unit = {
    val m: Map[Int, Double] = Range(0, 1000000).map(x => (x, x.toDouble)).toMap
    println(SizeEstimator.estimate(m))
  }

//  def test6(): Unit = {
//    implicit val sc: SparkContext = sparkSetup(this)
//
//    val sw = new StopWatch
//
//    val r1 = sc.range(0, 100*1000000)
//    //println(r1.estimateSize())
//    //println(r1.count())
//    //println(r1.map(x => (x,x)).count())
//    //println(r1.reduce(_+_))
//
////    r1.defaultPersist()
////    r1.force()
//
////    r1.checkpoint()
////    r1.force()
//
////    r1.sample(withReplacement = false, 0.01).force()
//
//    //println(r1.sample(withReplacement = false, 0.1).estimateSize())
//
//    //r1.repartition(8).force()
//
////    val r2 = r1.map(x => (x,x))
//////    println(r2.estimateSize())
////    r2.join(r2).force()
//
//    sw.done()
//  }



//  def test5(): Unit = {
//
//    implicit val sc: SparkContext = sparkSetup(this)
//
//
//    //val s: RDD[(Long, Long)] = sc.range(0, 1000000).map(x => (x,x))
//    val s: RDD[(Long, Any)] = sc.range(0, 1000000).map(x => (x,x))
//    //val s: RDD[Any] = sc.range(0, 1000000).map(x => (x,x))
//
//    val s2 = s.map(x=>x)
//
//    //s.localCheckpoint()
//    s2.checkpoint()
//
//    s2.force()
//
//    println(s2.map(x=>x).count())
//  }



//  def test4(): Unit = {
//
//    implicit val sc: SparkContext = sparkSetup(this)
//
//    //    val r: RDD[_] = sc.emptyRDD[Int]
//    //
//    //    val r2 = r.map(x => x)
//
//    val s: Seq[RDD[(Int, _)]] = Seq(sc.emptyRDD[(Int, Long)].asInstanceOf[(Int, _)], sc.emptyRDD[(Int, String)].asInstanceOf[(Int, _)])
//    //val s: Seq[RDD[_]] = Seq(sc.emptyRDD[(Int, Long)], sc.emptyRDD[(Int, String)])
//
//
//    val o = sc.emptyRDD[(Int, Any)]
//
//    val s2 = s.map(r => new PairRDDFunctions(r) join o)
//
//    s2.foreach(_.collect())
//  }


//    def test3(): Unit = {
//
//    implicit val sc: SparkContext = sparkSetup(this)
//
////    val r: RDD[_] = sc.emptyRDD[Int]
////
////    val r2 = r.map(x => x)
//
//    //val s: Seq[RDD[(Int, _)]] = Seq(sc.emptyRDD[(Int, Long)], sc.emptyRDD[(Int, String)])
//    //val s: Seq[RDD[_]] = Seq(sc.emptyRDD[(Int, Long)], sc.emptyRDD[(Int, String)])
//
//    val s: Seq[RDD[_]] = Seq(sc.parallelize(Seq((1, 2L))), sc.emptyRDD[(Int, String)])
//
//
//    val o = sc.parallelize(Seq((1, true)))
//
//    val s2 = s.map(r => r.asInstanceOf[RDD[(Int, Any)]] join o)
//
//    s2.foreach(_.print())
//  }




//  def test2(): Unit = {
//
//    implicit val sc: SparkContext = sparkSetup(this)
//
//    class WrappedRDD[T](val rdd: RDD[T]) {
//      type _T = T
//    }
//
////    val a: WrappedRDD[Long] = ???
////
////    val b: RDD[a._T] = ???
//
//    val s: Seq[WrappedRDD[_]] = Seq(new WrappedRDD(sc.emptyRDD[Long]), new WrappedRDD(sc.emptyRDD[String]))
//
//    s.map(wrdd => {
//      val rdd: RDD[wrdd._T] = wrdd.rdd
//      //val rdd = wrdd.rdd
//    })
//  }

//  def test1(): Unit = {
//    implicit val sc: SparkContext = sparkSetup(this)
//
//    val cond = sc.emptyRDD[(Long,Boolean)]
//
//    trait ~~>[F[_], G[_]] extends Poly1 {
//      def apply[T: ClassTag](f : F[(Long,T)]) : G[(Long,(T,Boolean))]
//      implicit def caseUniv[T: ClassTag]: Case.Aux[F[(Long,T)], G[(Long,(T,Boolean))]] = at[F[(Long,T)]](apply(_))
//    }
//
//    // choose is a function from Sets to Options with no type specific cases
//    object foo extends (RDD ~~> RDD) {
//      def apply[T: ClassTag](r: RDD[(Long,T)]) = new PairRDDFunctions(r).join(cond)
//    }
//
//    val hl = sc.emptyRDD[(Long, String)] :: sc.emptyRDD[(Long, Int)] :: HNil
//
//    hl.map(foo)
//
//
//
//    def f(ahl: HList): Unit = {
//      ahl.map(foo)
//    }
//
//    f(hl)
//
//  }

}
