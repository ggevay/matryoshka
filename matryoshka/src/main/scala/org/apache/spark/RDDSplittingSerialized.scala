package org.apache.spark

import java.io.{DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import com.twitter.chill.{Input, Kryo, Output}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{KryoSerializer, SerializationStream}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

import scala.collection.{immutable, mutable}
import scala.collection.mutable.{ArrayBuffer, Builder}
import scala.reflect.ClassTag

object RDDSplittingSerialized {

  implicit class groupByIntoRDDsAble[T: ClassTag](val in: RDD[T]) {
    def groupByIntoRDDs[K: ClassTag](f: T => K): Seq[(K, RDD[T])] = {
      if (in.getStorageLevel == StorageLevel.NONE) println("Warning: Input of groupByIntoRDDs is not cached!")
      val inter = GroupByIntoRDDsIntermediate(in)(f)
      val keys = in.map(f).distinct(1).collect()
      keys.map(k => (k, inter.getRDDForKey(k)))
    }
  }

  class GroupByIntoRDDsIntermediate[K: ClassTag, T: ClassTag](private val partitionsRDD: RDD[Map[K, ChunkedByteBuffer]])
    extends RDD[T](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

    override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

    override def compute(part: Partition, context: TaskContext): Iterator[T] = {
      firstParent[Map[K, Seq[T]]].iterator(part, context).next().values.flatten.iterator
    }

    def getRDDForKey(k: K): RDD[T] = {
      partitionsRDD.flatMap(m => {
//        val kryo = new KryoSerializer(new SparkConf()).newKryo //todo: this might be slow
//        val inp = new Input(cbb.toInputStream())
//        val outBuf = new ArrayBuffer[T]()
//        val cl = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
//        while(!inp.eof()) {
//          //outBuf += kryo.readObject(inp, cl)
//          //outBuf += kryo.readObjectOrNull(inp, cl)
//          outBuf += kryo.readClassAndObject(inp).asInstanceOf[T]
//        }
//        outBuf

        //        val ser = new KryoSerializer(new SparkConf()).newInstance()
        //        val cbb = m(k)
        //        val deserStr = ser.deserializeStream(cbb.toInputStream())
        //        deserStr.asIterator.asInstanceOf[Iterator[T]]

        val cbb = m(k)
        val inp = new DataInputStream(cbb.toInputStream())
        val outBuf = new ArrayBuffer[T]()
        for( i <- 1 to cbb.size.asInstanceOf[Int] / 16){ //TODO: vigyazat
          val f1 = inp.readLong()
          val f2 = inp.readLong()
          val tpl = Tuple2(f1, f2)
          outBuf += tpl.asInstanceOf[T]
        }
        outBuf
      })
    }
  }

  object GroupByIntoRDDsIntermediate {
    def apply[K: ClassTag, T: ClassTag](in: RDD[T])(f: T => K): GroupByIntoRDDsIntermediate[K, T] = {

//      val inp = new DataInputStream(cbbos.toChunkedByteBuffer.toInputStream())



      val partitions = in.mapPartitions(iter => {
//        val kryo = new KryoSerializer(new SparkConf()).newKryo
        //val ser = new KryoSerializer(new SparkConf()).newInstance()

        val m = mutable.Map.empty[K, (ChunkedByteBufferOutputStream, Output, SerializationStream, DataOutputStream)]
        for (elem <- iter) {
          val key = f(elem)
          val bldr = m.getOrElseUpdate(key, {
            val cbbos = new ChunkedByteBufferOutputStream(32 * 1024, ByteBuffer.allocate)
            //val out = new Output(cbbos)
            //val serstr = ser.serializeStream(cbbos)
            val dostr = new DataOutputStream(cbbos)
            (cbbos, null, null, dostr)
          })
          //kryo.writeObject(bldr._2, elem) // todo: step into and verify that the classname is not written
//          kryo.writeClassAndObject(bldr._2, elem)
          //bldr._3.writeObject(elem)

          elem match {
            case (x: Long, y: Long) =>
              bldr._4.writeLong(x)
              bldr._4.writeLong(y)
          }
        }
        val b = immutable.Map.newBuilder[K, ChunkedByteBuffer]
        for ((k, v) <- m) {
          v._1.close()
          b += ((k, v._1.toChunkedByteBuffer))
        }
        Iterator(b.result)
      },
        preservesPartitioning = false)
        .persist(StorageLevel.MEMORY_ONLY)

      new GroupByIntoRDDsIntermediate[K, T](partitions)
    }
  }
}
