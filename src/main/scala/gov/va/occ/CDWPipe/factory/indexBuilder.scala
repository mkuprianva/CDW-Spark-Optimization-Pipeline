package gov.va.occ.CDWPipe.factory

import gov.va.occ.CDWPipe.structs.index.{indexWrapper, indexWrapperInt, indexWrapperLong}
import gov.va.occ.CDWPipe.structs.index.{joinType, innerJoin}
import gov.va.occ.CDWPipe.utils.{ArrayIntersect, BloomFilter}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.ArraySeq

/**
  * Builds local index structures from DataFrames.
  *
  * This runs **after** filtering so the collected arrays stay small enough to
  * fit in driver memory.  For inner-join indices the two sides are intersected
  * locally (two-pointer or galloping, auto-dispatched) instead of performing a
  * Spark join, which avoids a shuffle stage.
  */
object indexBuilder {

  def extractIndex(
      spark: SparkSession,
      leftDF: Option[DataFrame],
      rightDF: Option[DataFrame],
      joinType: joinType,
      columnName: String,
      indexType: String,
      fpp: Double
  ): indexWrapper = {
    import spark.implicits._

    joinType match {
      case `innerJoin` =>
        if (leftDF.isEmpty || rightDF.isEmpty) {
          throw new IllegalArgumentException("Inner join requires both left and right DataFrames")
        }

        indexType match {
          case "Int" =>
            val leftIndex  = ArraySeq.unsafeWrapArray(leftDF.get.select(columnName).as[Int].collect().sorted)
            val rightIndex = ArraySeq.unsafeWrapArray(rightDF.get.select(columnName).as[Int].collect().sorted)
            val intersection = ArrayIntersect.intersectInt(leftIndex, rightIndex)
            val arr = intersection.toArray
            val bloom = BloomFilter.fromInts(arr, fpp)
            indexWrapperInt(0, arr, bloom, new java.sql.Timestamp(System.currentTimeMillis()))

          case "Long" =>
            val leftIndex  = ArraySeq.unsafeWrapArray(leftDF.get.select(columnName).as[Long].collect().sorted)
            val rightIndex = ArraySeq.unsafeWrapArray(rightDF.get.select(columnName).as[Long].collect().sorted)
            val intersection = ArrayIntersect.intersectLong(leftIndex, rightIndex)
            val arr = intersection.toArray
            val bloom = BloomFilter.fromLongs(arr, fpp)
            indexWrapperLong(0L, arr, bloom, new java.sql.Timestamp(System.currentTimeMillis()))

          case _ =>
            throw new IllegalArgumentException(s"Unsupported index type: $indexType")
        }

      case _ =>
        throw new IllegalArgumentException(s"Unsupported join type: $joinType")
    }
  }
}