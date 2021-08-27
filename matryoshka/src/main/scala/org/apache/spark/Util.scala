package org.apache.spark

import org.apache.spark.util.collection.CompactBuffer

import scala.reflect.ClassTag

object Util {
  def createCompactBuffer[T: ClassTag](s: Seq[T]): CompactBuffer[T] = {
    CompactBuffer[T]() ++= s
  }
}
