package org.apache.spark

import scala.reflect.ClassTag
import org.apache.spark.rdd.{RDD, EmptyRDD}
import org.apache.spark.storage.StorageLevel

/**
 * Created by U0159515 on 5/19/15.
 */
object Flyby {

  def getPartitionsAsLocalArray[T](rdd:RDD[T])(implicit ev:ClassTag[T]):Iterator[Array[T]] = {
    def collectPartition(p: Int): Array[T] = {
      rdd.sparkContext.runJob(rdd, (iter: Iterator[T]) => iter.toArray, Seq(p), allowLocal = false).head
    }
    (0 until rdd.partitions.length).iterator.map(i => collectPartition(i))
  }

  implicit class Flyby [L](left:RDD[L]) {
    def flyby[R,O] (right:RDD[R], fold0: (L,R)=>O, foldFun: (L,R,O) => O, storageLevel:StorageLevel=StorageLevel.MEMORY_AND_DISK)(implicit ev:ClassTag[O], ev2: ClassTag[L], ev3: ClassTag[R]) = {
      var reduction:RDD[O] = null
      val leftLocal = getPartitionsAsLocalArray(left)
      for(part <- leftLocal) {
        if(part.length > 0) { //skip if partition is empty
          val flyer = right.sparkContext broadcast (part)
          val nextReduction = if (reduction == null) {
            //do initialization before folding
            right.map { case rr =>
              val init = fold0(flyer.value.head, rr)
              flyer.value.tail.foldLeft(init) { case (acc, nextflyer) => foldFun(nextflyer, rr, acc)}
            }
          } else {
            //just fold
            right.zip(reduction).map { case (rr, red) =>
              flyer.value.foldLeft(red) { case (acc, nextflyer) => foldFun(nextflyer, rr, acc)}
            }
          }.persist(storageLevel)

          nextReduction.count
          flyer.unpersist (false)
          if (reduction != null) reduction.unpersist(false)
          reduction = nextReduction
        }
      }
      if(reduction!=null) reduction else new EmptyRDD[O](right.sparkContext)
    }
  }
}
