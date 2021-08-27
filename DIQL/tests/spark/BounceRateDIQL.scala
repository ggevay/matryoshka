import Test.{Customer, Order}
import org.apache.spark.SparkContext
import de.tuberlin.dima.matryoshka.Util._
import edu.uta.diql._
import org.apache.log4j.{Level, LogManager}
import org.apache.spark._
import org.apache.spark.rdd._

object Test {

  case class Customer ( name: String, cid: Int, account: Float )

  case class Order ( oid: Int, cid: Int, price: Float )

  def main(args: Array[String]): Unit = {

    val skewed = args.length == 3

    val totalSize = args(0).toLong
    val numGroups = args(1).toInt
    val exponent =
      if (skewed) {
        args(2).toDouble
      } else {
        0
      }

    implicit val sc: SparkContext = sparkSetup(this)

    val visitsAll =
      if (skewed) {
        getBounceRateSkewedRandomInput(totalSize, numGroups, exponent)
      } else {
        getBounceRateRandomInput(totalSize, numGroups)
      }

    val stopWatch = new StopWatch

    explain(true)







    //    q("""
    //     4::5::List(1,2)
    //     """).foreach(println)

//    q("""
//      visitsAll
//     """).foreach(println)

//    q("""
//      select (g,v)
//      from (g,v) <- visitsAll
//     """).foreach(println)


//    q("""
//      select distinct v
//      from (g,v) <- visitsAll
//     """).foreach(println)

//    q("""
//      select (gid,count/vid)
//      from (gid,vid) <- visitsAll
//      group by gid
//     """).foreach(println)

    // Nested output:
//    q("""
//      select (gid,vid)
//      from (gid,vid) <- visitsAll
//      group by gid
//     """).foreach(println)

//    q("""
//      select (gid,
//        select distinct vid
//        from vid <- vids
//      )
//      from (gid,vids) <- visitsAll
//      group by gid
//     """).foreach(println)




//    q("""
//      select (gid,
//        (select distinct vid from vid <- vids)
//      )
//      from (gid,vids) <- visitsAll
//      group by gid
//     """).foreach(println)

//    q("""
//      select (gid,
//        (count/vids)
//      )
//      from (gid,vids) <- visitsAll
//      group by gid
//     """).foreach(println)

    // osztas proba:
//    q("""
//      select (gid,
//        (count/vids).toDouble / ((count/(select distinct vid from vid <- vids)).toDouble)
//      )
//      from (gid,vids) <- visitsAll
//      group by gid
//     """).foreach(println)





//    // num of unique:
//    q("""
//      select (gid,
//        (count/(select distinct vid from vid <- vids))
//      )
//      from (gid,vids) <- visitsAll
//      group by gid
//     """).foreach(println)

//
//    q("""
//      select (gid,
//        select (vid,count/c)
//        from (vid,c) <- (select (v,1L) from v <- visits)
//        group by vid
//      )
//      from (gid,visits) <- visitsAll
//      group by gid
//     """).foreach(println)


//    // Bounced filtered:
//    q("""
//      select (gid,
//        select vid
//        from (vid,c) <- (select (v,1L) from v <- visits)
//        group by vid
//        having count/c == 1
//      )
//      from (gid,visits) <- visitsAll
//      group by gid
//     """).foreach(println)

//    // numBounces:
//    q("""
//      select (gid,
//        count/
//        (select vid
//        from (vid,c) <- (select (v,1L) from v <- visits)
//        group by vid
//        having count/c == 1)
//      )
//      from (gid,visits) <- visitsAll
//      group by gid
//     """).foreach(println)




//    // Bounce Rate with gid
//    q("""
//      select (gid,
//        (count/
//          (select vid
//          from (vid,c) <- (select (v,1L) from v <- visits)
//          group by vid
//          having count/c == 1)
//        ).toDouble
//        /
//        (count/(select distinct vid from vid <- visits)).toDouble
//      )
//      from (gid,visits) <- visitsAll
//      group by gid
//     """).foreach(println)
//
//// Compiled code:
//    visitsAll.groupByKey().mapValues({
//      case (diql$1 @ _) => diql$1.flatMap(((x$macro$104: Long) => x$macro$104 match {
//        case (diql$9 @ _) => {
//          val x$4 = (((scala.Tuple2(diql$9, 1L): @scala.unchecked): scala.Tuple2[Long, Long]) match {
//            case scala.Tuple2((vid @ _), (c @ _)) => scala.Tuple2(vid, c)
//          });
//          val vid = x$4._1;
//          val c = x$4._2;
//          List(scala.Tuple2(vid, c))
//        }
//      })).groupBy(((x$7) => x$7._1)).mapValues(((x$6) => x$6.map(((x$5) => x$5._2)))).flatMap(((x$macro$108: scala.Tuple2[Long, Traversable[Long]]) => x$macro$108 match {
//        case scala.Tuple2((diql$5 @ _), (diql$6 @ _)) => if (diql$6.map(((x$macro$111: Long) => x$macro$111 match {
//          case (diql$7 @ _) => 1L
//        })).foldLeft[Long](0)(((x: Long, y: Long) => x.+(y))).==(1))
//          List(1L)
//        else
//          Nil
//      })).foldLeft[Long](0)(((x: Long, y: Long) => x.+(y))).toDouble./(diql$1.map(((x$macro$116: Long) => x$macro$116 match {
//        case (diql$12 @ _) => scala.Tuple2(diql$12, 0)
//      })).groupBy(((x$10) => x$10._1)).mapValues(((x$9) => x$9.map(((x$8) => x$8._2)))).map(((x$macro$118: scala.Tuple2[Long, Traversable[Int]]) => x$macro$118 match {
//        case scala.Tuple2((diql$10 @ _), _) => 1L
//      })).foldLeft[Long](0)(((x: Long, y: Long) => x.+(y))).toDouble)
//    })





    // !! Bounce Rate !!
    q("""
      select (
        (count/
          (select vid
          from (vid,c) <- (select (v,1L) from v <- visits)
          group by vid
          having count/c == 1)
        ).toDouble
        /
        (count/(select distinct vid from vid <- visits)).toDouble
      )
      from (gid,visits) <- visitsAll
      group by gid
     """).foreach(println)

//    // Scala code:
//    visitsAll.groupByKey().map({
//      case scala.Tuple2((gid @ _), (diql$1 @ _)) => diql$1.flatMap(((x$macro$104: Long) => x$macro$104 match {
//        case (diql$9 @ _) => {
//          val x$4 = (((scala.Tuple2(diql$9, 1L): @scala.unchecked): scala.Tuple2[Long, Long]) match {
//            case scala.Tuple2((vid @ _), (c @ _)) => scala.Tuple2(vid, c)
//          });
//          val vid = x$4._1;
//          val c = x$4._2;
//          List(scala.Tuple2(vid, c))
//        }
//      })).groupBy(((x$7) => x$7._1)).mapValues(((x$6) => x$6.map(((x$5) => x$5._2)))).flatMap(((x$macro$108: scala.Tuple2[Long, Traversable[Long]]) => x$macro$108 match {
//        case scala.Tuple2((diql$5 @ _), (diql$6 @ _)) => if (diql$6.map(((x$macro$111: Long) => x$macro$111 match {
//          case (diql$7 @ _) => 1L
//        })).foldLeft[Long](0)(((x: Long, y: Long) => x.+(y))).==(1))
//          List(1L)
//        else
//          Nil
//      })).foldLeft[Long](0)(((x: Long, y: Long) => x.+(y))).toDouble./(diql$1.map(((x$macro$116: Long) => x$macro$116 match {
//        case (diql$12 @ _) => scala.Tuple2(diql$12, 0)
//      })).groupBy(((x$10) => x$10._1)).mapValues(((x$9) => x$9.map(((x$8) => x$8._2)))).map(((x$macro$118: scala.Tuple2[Long, Traversable[Int]]) => x$macro$118 match {
//        case scala.Tuple2((diql$10 @ _), _) => 1L
//      })).foldLeft[Long](0)(((x: Long, y: Long) => x.+(y))).toDouble)
//    })



    stopWatch.done()
  }
}
