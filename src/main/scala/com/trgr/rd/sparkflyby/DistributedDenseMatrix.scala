package com.trgr.rd.sparkflyby

/**
 * Created by U0159515 on 6/6/15.
 */

import breeze.linalg._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Flyby._
import org.apache.spark.SparkContext

object DistributedDenseMatrix {

  case class Blocking(m:Int, n:Int, mBlocks:Int, nBlocks:Int) {
    require(mBlocks <= m && nBlocks <= n, "block size must be less than overall size")
    def getBlockRange(index:(Int,Int)) : ((Int,Int), (Int,Int)) = {
      val (rowId, colId) = index
      if(rowId<0 || rowId >= mBlocks) throw new IndexOutOfBoundsException("requested blkRowId out of range")
      if(colId<0 || colId >= nBlocks) throw new IndexOutOfBoundsException("requested blkColId out of range")
      val rowRan = (rowId*m/mBlocks, math.min( (rowId+1)*m/mBlocks, m) )
      val colRan = (colId*n/nBlocks, math.min( (colId+1)*n/nBlocks, n) )
      (rowRan, colRan)
    }

    def getBlockId(globalIndex:(Int,Int)) : (Int,Int) = {
      val (i,j) = globalIndex
      if(i < 0 || i >= m) throw new IndexOutOfBoundsException("requested row index out of range")
      if(j < 0 || j >= n) throw new IndexOutOfBoundsException("requested col index out of range")
      (i*mBlocks/m, j*nBlocks/n)
    }

    def getBlockSize(blockIndex: (Int,Int)) = {
      val blkRange = getBlockRange(blockIndex)
      (blkRange._1._2 - blkRange._1._1, blkRange._2._2 - blkRange._2._1)
    }

    def getGlobalIndex(localIndex:(Int,Int), blockIndex:(Int,Int)) = {
      val blockRange = getBlockRange(blockIndex)
      (blockRange._1._1 + localIndex._1, blockRange._2._1 + localIndex._2)
    }

    def getLocAndBlkInds(globalIndex:(Int,Int)) = {
      val blk = getBlockId(globalIndex)
      val range = getBlockRange(blk)
      val loc = (globalIndex._1 - range._1._1, globalIndex._2 - range._2._1)
      (loc, blk)
    }


  }

  type DM = DenseMatrix[Double]
  type MatrixBlock = ((Int,Int), DM)


  object MatrixBlockOps{
    def dot(left: MatrixBlock, right:MatrixBlock) = new MatrixBlock((left._1._1, right._1._2), left._2*right._2)
    //def addInPlaceLeft(left:MatrixBlock, right:MatrixBlock) = {if(left._2.rows != right._2.rows || left._2.cols != right._2.cols) throw new UnsupportedOperationException("Dimensions must match"); left._2 += right._2; left}
    //def accpxy(acc:MatrixBlock, x:MatrixBlock, y:MatrixBlock) = {if(x._2.cols != y._2.rows || acc._2.rows != x._2.rows || acc._2.cols != y._2.cols) throw new UnsupportedOperationException("Dimensions must match"); acc._2 += x._2 * y._2; acc}

    //indexes into acc based on x's global row index
    def accpxyInner(acc:MatrixBlock, x:MatrixBlock, xBlocking:Blocking, y:MatrixBlock) = {
      if(x._2.cols != y._2.rows || y._2.cols != acc._2.cols) throw new UnsupportedOperationException("Dimensions must match");
      val xRows = xBlocking.getBlockRange(x._1)._1
      if(xRows._2 > acc._2.rows) throw new IndexOutOfBoundsException("xRows too large for accumulator")
      acc._2(xRows._1 until xRows._2, ::) += x._2 * y._2
      acc
    }

    //indexes into y based on x's global col index
    def accpxyOuter(acc:MatrixBlock, x:MatrixBlock, xBlocking:Blocking, y:MatrixBlock) = {
      if(y._2.cols != acc._2.cols || x._2.rows != acc._2.rows) throw new UnsupportedOperationException("Dimensions must match");
      val xCols = xBlocking.getBlockRange(x._1)._2
      if(xCols._2 > y._2.rows) throw new IndexOutOfBoundsException("xCols too large for y")
      val ySubBlock = y._2(xCols._1 until xCols._2, ::)
      acc._2 += x._2 * ySubBlock
      acc
    }
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

    def getBlocking = blocking

    def shuffleMultiply(right: DenseBlockMatrix, nPartitions:Option[Int] = None) = {
      if(blocking.n != right.blocking.m || blocking.nBlocks != right.blocking.mBlocks) throw new UnsupportedOperationException("incompatible matrix blocking")
      val newBlocking = Blocking(blocking.m, right.blocking.n, blocking.mBlocks, right.blocking.nBlocks)
      val part = new ColMajorPartitioner(nPartitions.getOrElse(blocks.partitions.length), newBlocking)

      val leftShuffle = blocks.flatMap {case ((blkRow, blkCol), values) =>
        (0 until blocking.nBlocks).map{ j =>
          (((blkRow, j),blkCol), values)
        }
      }

      val rightShuffle = right.blocks.flatMap {case ((blkRow, blkCol), values) =>
        (0 until right.blocking.mBlocks).map{i =>
          (((i, blkCol), blkRow), values)
        }
      }

      val newBlocks = leftShuffle.join(rightShuffle, part)
        .mapPartitions(it => it.map{ case (((rowId, colId), matchId), (lblk, rblk)) => ((rowId, colId), lblk*rblk)}, true) //preserves partitioning because partitioner is agnostic about third element in key
        .reduceByKey(part, (left:DM, right:DM) => {left += right; left})
//        .mapPartitions(it => it.map{ case (((rowId, colId), matchId), blks) => ((rowId, colId), blks)}, true) //preserves partitioning because partitioner is agnostic about third element in key
//        .combineByKey( (blks: (DM, DM)) => blks._1 * blks._2,
//          (acc: DM, right: (DM, DM)) => {acc += right._1 * right._2; acc},
//          (left: DM, right: DM) => {left += right; left}
//        )
      DenseBlockMatrix(newBlocking, newBlocks)
    }

    def cartesianMultiply(right:DenseBlockMatrix, nPartitions:Option[Int] = None) = {
      if(blocking.n != right.blocking.m || blocking.nBlocks != right.blocking.mBlocks) throw new UnsupportedOperationException("incompatible matrix blocking")
      val newBlocking = Blocking(blocking.m, right.blocking.n, blocking.mBlocks, right.blocking.nBlocks)
      val part = new ColMajorPartitioner(nPartitions.getOrElse(blocks.partitions.length), newBlocking)
      val newBlocks = blocks.cartesian(right.blocks).flatMap{ case (lblk, rblk) =>
        if(lblk._1._2 == rblk._1._1)
          Some( MatrixBlockOps.dot(lblk,rblk) )
        else None
      }.reduceByKey(part, (left:DM, right:DM) => {left += right; left})

      DenseBlockMatrix(newBlocking, newBlocks)

    }

    def flyByOuter(right:DenseBlockMatrix, storageLevel:StorageLevel = StorageLevel.MEMORY_AND_DISK) = {
      import MatrixBlockOps._
      if(blocking.mBlocks>1 || right.blocking.mBlocks>1)
        throw new UnsupportedOperationException("flyByOuter only implemented for column-decomposed matrices")
      val newBlocking = Blocking(blocking.m, right.blocking.n, blocking.mBlocks, right.blocking.nBlocks)
      val newBlocks = blocks.flyby(right.blocks,
        (left:MatrixBlock, right:MatrixBlock) => {
          val acc = ((left._1._1, right._1._2), DenseMatrix.zeros[Double](left._2.rows, right._2.cols))
          accpxyOuter(acc, left, blocking, right)
        },
        (left:MatrixBlock, right:MatrixBlock, acc:MatrixBlock) => accpxyOuter(acc, left, blocking, right)
      )
      DenseBlockMatrix(newBlocking, newBlocks)
    }

    def flyByInner(right:DenseBlockMatrix, storageLevel:StorageLevel = StorageLevel.MEMORY_AND_DISK) = {
      import MatrixBlockOps._
      if(blocking.nBlocks>1 || right.blocking.mBlocks>1)
        throw new UnsupportedOperationException("flyByInner implemented for (row-decomposed) * (col-decomposed) matrices")
      val newBlocking = Blocking(blocking.m, right.blocking.n, 1, right.blocking.nBlocks)
      val newBlocks = blocks.flyby(right.blocks,
        (left:MatrixBlock, right:MatrixBlock) => {
          val acc = new MatrixBlock( (0, right._1._2), DenseMatrix.zeros[Double](blocking.m, right._2.cols)) //rowId always 0
          accpxyInner(acc, left, blocking, right)
        },
        (left:MatrixBlock, right:MatrixBlock, acc:MatrixBlock) => accpxyInner(acc, left, blocking, right)
      )
      DenseBlockMatrix(newBlocking, newBlocks)
    }

    def frobNormSqDiff(right:DenseBlockMatrix) = {
      if(blocking != right.blocking) throw new UnsupportedOperationException("Blocking mismatch.  Reblock one matrix to match")
      blocks.join(right.blocks).map{case (_, (left, right)) => left.data.zip(right.data).foldLeft(0.){case (acc, (lv, rv)) => {val diff = lv-rv; acc + diff*diff}}}.reduce(_+_)
    }
    def frobNormSq = {
      blocks.map{ case (id, vals) => vals.data.foldLeft(0.){case (acc, v) => acc + v*v}}.reduce(_+_)
    }

    def reblock(mBlocks:Int, nBlocks:Int, nPartitions:Option[Int] = None) = {
      val newBlocking = Blocking(blocking.m, blocking.n, mBlocks, nBlocks)
      val part = new ColMajorPartitioner(nPartitions.getOrElse(blocks.partitions.length), newBlocking)
      val newBlocks = blocks.flatMap{case (blkIndex, values) =>
        values.iterator.map{case (locIndex,v) =>
          val (newLoc, newBlk) = newBlocking.getLocAndBlkInds(blocking.getGlobalIndex(locIndex, blkIndex))
          (newBlk, (newLoc,v))
        }
      }
      .groupByKey(part)
      .map { case (blkId, ijvSeq) =>
        //val (nRows, nCols) = ijvSeq.foldLeft((0, 0)) { case (acc, (nextInd, _)) => (math.max(acc._1, nextInd._1), math.max(acc._2, nextInd._2))}
        val sz = newBlocking.getBlockSize(blkId)
        val newMat = DenseMatrix.zeros[Double](sz._1, sz._2)
        ijvSeq.foreach{case ((i,j),v) => newMat(i,j) = v}    //hopefully indices from each node arrive in stride order
        //if (ijvArray.length != nRows * nCols) throw new Exception("Did not receive complete matrix in shuffle")
        //new MatrixBlock(blkId, DenseMatrix(nRows, nCols, ijvArray.sortBy { case ((i, j), v) => j * nCols + i }.map(_._2) ))
        new MatrixBlock(blkId, newMat)
      }
      DenseBlockMatrix(newBlocking, newBlocks)
    }
    def persist(storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY) = {blocks.persist(storageLevel); this}
    def count = blocks.count
  }
  object DenseBlockMatrix {
    def newRandomMatrix(m:Int, n:Int, mBlocks:Int, nBlocks:Int, nPartitions:Int, sc: SparkContext) = {
      val blocking = new Blocking (m, n, mBlocks, nBlocks)
      val part = new ColMajorPartitioner(nPartitions, blocking)
      val blocks = sc.parallelize(0 until mBlocks*nBlocks, nPartitions).map{i =>
        val blockId = (i%nBlocks, i/nBlocks)
        val blockSize = blocking.getBlockSize(blockId)
        val entries = Array.fill(blockSize._1 * blockSize._2)(util.Random.nextGaussian)
        (blockId, new DenseMatrix(blockSize._1, blockSize._2, entries))
      }.partitionBy(part)
      DenseBlockMatrix(blocking, blocks)
    }
  }




}
