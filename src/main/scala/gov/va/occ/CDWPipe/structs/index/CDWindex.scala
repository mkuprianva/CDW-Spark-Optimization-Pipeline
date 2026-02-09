package gov.va.occ.CDWPipe.structs.index
import scala.collection.mutable.ArrayBuffer
import gov.va.occ.CDWPipe.utils.BloomFilter

sealed trait indexWrapper

sealed trait joinType
case object innerJoin extends joinType
case object leftJoin extends joinType
case object rightJoin extends joinType
case object fullJoin extends joinType
case object leftAntiJoin extends joinType
case object leftSemiJoin extends joinType

case class indexWrapperInt(
    id: Int,
    index: Array[Int],
    bloom: BloomFilter,
    lastUpdated: java.sql.Timestamp
) extends indexWrapper


case class indexWrapperLong(
    id: Long,
    index: Array[Long],
    bloom: BloomFilter,
    lastUpdated: java.sql.Timestamp
) extends indexWrapper

case class CDWindex(
  name: String,
  leftTable: String,
  rightTable: String,
  joinType: joinType,
  indexType: String,
  index: indexWrapper
  
)

