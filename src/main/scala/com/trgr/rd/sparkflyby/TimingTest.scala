package com.trgr.rd.sparkflyby

import java.io._
import DistributedDenseMatrix.{DenseBlockMatrix => DM}
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by twvacek on 6/9/2015.
 */
object TimingTest {

  def time[R](block: => R): (R,Double) = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime
    val etime = (t1-t0)/1e9
    (result, etime)
  }



  def main(args:Array[String]) = {
    val conf = new SparkConf().setAppName("MatrixTimingTest")
    val sc = new SparkContext(conf)

    val printer = new PrintWriter(new BufferedWriter(new FileWriter(args(0))))
    val tail = args.tail
    val matDim = tail(0).toInt
    val nBlocks = tail.tail.map(_.toInt)
    for(n <- nBlocks) {
      val rowBlocked = DM.newRandomMatrix(matDim, matDim, 1, n*n, n*n, sc)
      val colBlocked = DM.newRandomMatrix(matDim, matDim, n*n, 1, n*n, sc)
      rowBlocked.persist().count
      colBlocked.persist().count
      val (cartFrob, cartTime) = time{rowBlocked.cartesianMultiply(colBlocked).frobNormSq}
      printer.println("cartesian %d %d\t%f\t%e".format(matDim, n, cartTime, cartFrob))
      printer.flush()
      val ((matin, fbInnerFrob), fbInnerTime) = time{
        val mat = rowBlocked.flyByInner(colBlocked)
        val frobNorm = mat.frobNormSq
        (mat, frobNorm)
      }
      printer.println("flybyInner %d %d\t%f\t%e".format(matDim, n, fbInnerTime, fbInnerFrob))
      printer.flush
      matin.unpersist
      rowBlocked.unpersist

      val ((matout, fbOuterFrob), fbOuterTime) = time{
        val mat = colBlocked.flyByOuter(colBlocked)
        val frobNorm = mat.frobNormSq
        (mat, frobNorm)
      }
      printer.println("flybyOuter %d %d\t%f\t%e".format(matDim, n, fbOuterTime, fbOuterFrob))
      printer.flush
      matout.unpersist
      colBlocked.unpersist

      val equalBlocked = DM.newRandomMatrix(matDim, matDim, n, n, n*n, sc)
      equalBlocked.persist().count
      val(shufNorm, shufTime) = time{equalBlocked.shuffleMultiply(equalBlocked).frobNormSq}
      printer.println("shuffle %d %d\t%f\t%e".format(matDim, n, shufTime, shufNorm))
      printer.flush()
      equalBlocked.unpersist

    }

  }

}
