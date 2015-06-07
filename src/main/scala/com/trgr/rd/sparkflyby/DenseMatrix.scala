package com.trgr.rd.sparkflyby

/**
 * Created by U0159515 on 5/19/15.
 */

import breeze.linalg._
import breeze.numerics._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Flyby._


case class Blocking(m:Int, n:Int, mBlocks:Int, nBlocks:Int) {}
//  def getRowRange(rowId:Int) = {
//    if(rowId<0 || rowId >= mBlocks) throw new Exception("requested rowId out of range")
//    blkSize = if(m%mBlocks==0) m/mBlocks else m/(mBlocks-1)
//    (rowId*blkSize, math.min( (rowId+1)*blkSize, m) )
//  }
//  def getColRange(colId:Int) = ...
//}
////((rowId, colId), values)
//type MatrixBlock = ((Int,Int), DenseMatrix[Double])
//
//object MatrixBlockOps{
//  def dot(left: MatrixBlock, right:MatrixBlock) = new MatrixBlock((left._1._1, right._1._2), left._2*right._2)
//  def addInPlaceLeft(left:MatrixBlock, right:MatrixBlock) = {if(left.rows != right.rows || left.cols != right.cols) throw new UnsupportedOperationException("Dimensions must match"); left += right; left}
//  def accpxy(acc:MatrixBlock, x:MatrixBlock, y:MatrixBlock) = {if(x._2.cols != y._2.rows || acc._2.rows != x._2.rows || acc._2.cols != y._2.cols) throw new UnsupportedOperationException("Dimensions must match")}
//}

//index errors not checked
//rowIds/colIds contains (rowIds._1 until rowIds._2) of the global indices stored in the block, with zero-indexing
case class MatrixBlock(bRowId:Int, bColId:Int, rowIds:(Int,Int), colIds:(Int,Int), values:DenseMatrix[Double]){
  def dot(right: MatrixBlock) = new MatrixBlock(bRowId, right.bColId, rowIds, right.colIds, values * right.values)
  def add(right: MatrixBlock) = new MatrixBlock(bRowId, bColId, rowIds, colIds, values + right.values)
  def addInPlace(right: MatrixBlock) = {values += right.values; this}
  def pxy(x: MatrixBlock, y: MatrixBlock) = {values += (x.values*y.values); this}
  def pxy(x: DenseMatrix[Double], y:DenseMatrix[Double]) = {values += x*y; this}
}

class ColMajorPartitioner(val nPartitions:Int, val blocking:Blocking) extends Partitioner {
  def numPartitions = nPartitions

  def getPartition(key: Any) = {
    key match {                                                                                              //can't be negative
      case ((rowId: Int, colId: Int), _) => nPartitions * (rowId + blocking.mBlocks * colId) / (blocking.mBlocks * blocking.nBlocks)    //keys from shuffleMultiply
      case (rowId: Int, colId: Int) => nPartitions * (rowId + blocking.mBlocks * colId) / (blocking.mBlocks * blocking.nBlocks)                   //keys from cartesianMultiply
      case _ => 0
    }
  }
  override def equals(other:Any) = {
    other match {
      case cmp: ColMajorPartitioner => cmp.nPartitions == nPartitions && cmp.blocking == blocking
      case _ => false
    }
  }
}

case class DenseBlockMatrix (blocking:Blocking, blocks: RDD[MatrixBlock]) {

  //def transpose = DenseBlockMatrix( nBlocks, mBlocks, blocks map {blk => MatrixBlock( blk.bColId, blk.bRowId, blk.colIds, blk.rowIds, blk.values.t)})

  //default col major partitioning
  def shuffleMultiply(right: DenseBlockMatrix, partitioner:Option[Partitioner] = None) = {
    val newBlocking = Blocking(blocking.m, right.blocking.n, blocking.mBlocks, right.blocking.nBlocks)
    val part = partitioner match {
      case Some(p) => p
      case None => new ColMajorPartitioner(blocks.partitions.length, newBlocking)
    }

    val leftShuffle = blocks.flatMap { blk =>
      (0 until blocking.nBlocks).map{ j =>
        (((blk.bRowId, j),blk.bColId), blk)
      }
    }

    val rightShuffle = right.blocks.flatMap {blk =>
      (0 until right.blocking.mBlocks).map{i =>
        (((i, blk.bColId), blk.bRowId), blk)
      }
    }

    val newBlocks = leftShuffle.join(rightShuffle, part)
      .map{case (((rowId, colId), blkId), blk) => ((rowId, colId), blk)}
      .combineByKey( (blks: (MatrixBlock, MatrixBlock)) => blks._1.dot(blks._2),
        (acc: MatrixBlock, right: (MatrixBlock, MatrixBlock)) => acc.pxy(right._1, right._2),
        (left: MatrixBlock, right: MatrixBlock) => left.addInPlace(right)
      ).map(_._2)

    DenseBlockMatrix(newBlocking, newBlocks)

  }

  //default col major partitioning
  def cartesianMultiply(right:DenseBlockMatrix, partitioner:Option[Partitioner]) = {
    val newBlocking = Blocking(blocking.m, right.blocking.n, blocking.mBlocks, right.blocking.nBlocks)
    val part = partitioner match {
      case Some(p) => p
      case None => new ColMajorPartitioner(blocks.partitions.length, newBlocking)
    }
    val newBlocks = blocks.cartesian(right.blocks).flatMap{case (lblk, rblk) =>
       if(lblk.bColId == rblk.bRowId) {
         val nblk = lblk.dot(rblk)
         Some( ((nblk.bRowId, nblk.bColId), nblk) )
       } else None
    }.partitionBy(part)
     .combineByKey( (blks:MatrixBlock) => blks,
        (acc:MatrixBlock, right:MatrixBlock) => acc.addInPlace(right),
        (left:MatrixBlock, right:MatrixBlock) => left.addInPlace(right)
      ).map(_._2)

    DenseBlockMatrix(newBlocking, newBlocks)

  }

  def flyByOuter(right:DenseBlockMatrix, storageLevel:StorageLevel = StorageLevel.MEMORY_AND_DISK) = {
    if(blocking.mBlocks>1 || right.blocking.mBlocks>1)
      throw new UnsupportedOperationException("flyByOuter only implemented for column-decomposed matrices")
    val newBlocking = Blocking(blocking.m, right.blocking.n, blocking.mBlocks, right.blocking.nBlocks)
    val newBlocks = blocks.flyby(right.blocks,
      (left:MatrixBlock, right:MatrixBlock) => {
        val rightSubBlock = right.values(left.colIds._1 until left.colIds._2, ::)
        MatrixBlock(left.bRowId, right.bColId, left.rowIds, right.colIds, left.values * rightSubBlock)    //left.bRowId is always 1
      },
      (left:MatrixBlock, right:MatrixBlock, acc:MatrixBlock) => {
        val rightSubBlock = right.values(left.colIds._1 until left.colIds._2, ::)
        acc.pxy(left,right)
      }
    )
    DenseBlockMatrix(newBlocking, newBlocks)
  }
  def flyByInner(right:DenseBlockMatrix, lRows:Int, storageLevel:StorageLevel = StorageLevel.MEMORY_AND_DISK) = {
    if(blocking.nBlocks>1 || right.blocking.mBlocks>1)
      throw new UnsupportedOperationException("flyByInner implemented for (row-decomposed) * (col-decomposed) matrices")
    val newBlocking = Blocking(blocking.m, right.blocking.n, 1, right.blocking.nBlocks)
    val newBlocks = blocks.flyby(right.blocks,
      (left:MatrixBlock, right:MatrixBlock) => {
        val newValues = DenseMatrix.zeros[Double](lRows, right.colIds._2 - right.colIds._1)
        newValues(left.rowIds._1 until left.rowIds._2,::) := left.values * right.values
        new MatrixBlock(1, right.bColId, (0,lRows), right.colIds, newValues)
      },
      (left:MatrixBlock, right:MatrixBlock, acc:MatrixBlock) => {
        acc.values(left.rowIds._1 until left.rowIds._2,::) := left.values * right.values;
        acc }
    )
    DenseBlockMatrix(newBlocking, newBlocks)
  }
}
