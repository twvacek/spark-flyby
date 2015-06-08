package com.trgr.rd.sparkflyby

import org.apache.spark.{SparkContext, SparkConf}
import SparkContext._
import DistributedDenseMatrix.{DenseBlockMatrix => DM}


/**
 * Created by twvacek on 6/7/2015.
 */
object LocalTest {

  def main(args:Array[String]) = {

    val master = "local[4]"

    val conf = new SparkConf().setAppName("HMMPartyFeatureGen").setMaster(master)
    conf.set("spark.executor.memory", "5g")
    conf.set("spark.driver.memory", "2g")
    val sc = new SparkContext(conf)
    val mat = DM.newRandomMatrix(500,500,5,5,5, sc)
    mat.persist()
    mat.count
    val prodShuf = mat.shuffleMultiply(mat)
    val prodCart = mat.cartesianMultiply(mat)
    //println("Entries in product matrix: " + prod.count)
    val frobNormDiff = prodShuf.frobNormSqDiff(prodCart)
    val frobNormShuf = prodShuf.frobNormSq

    val colPartitioned = mat.reblock(1,25)
    colPartitioned.persist().count
    val flybyOuterProd = colPartitioned.flyByOuter(colPartitioned)
    val frobNormDiffFlyOuter = prodShuf.frobNormSqDiff(flybyOuterProd.reblock(5,5))

    val rowPartitioned = mat.reblock(25,1)
    rowPartitioned.persist().count
    val flybyInnerProd = rowPartitioned.flyByInner(colPartitioned)
    val frobNormDiffFlyInner = prodShuf.frobNormSqDiff(flybyInnerProd.reblock(5,5))

    println("Frobenius norm sq of shuffle: " + "%e".format(frobNormShuf))
    println("Frobenius norm squared difference of cartesian v. shuffle: " + "%e".format(frobNormDiff))
    println("Frobenius norm squared difference of flybyOuter v. shuffle: " + "%e".format(frobNormDiffFlyOuter))
    println("Frobenious norm squared difference of flybyIntter v. shuffle: " + "%e".format(frobNormDiffFlyInner))
    ()
  }

}
