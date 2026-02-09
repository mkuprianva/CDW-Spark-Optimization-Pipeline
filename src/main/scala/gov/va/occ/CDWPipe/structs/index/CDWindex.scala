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

object joinType {
  /** Encode a joinType as a single-byte tag for binary serialization. */
  def toTag(jt: joinType): Byte = jt match {
    case `innerJoin`    => 0.toByte
    case `leftJoin`     => 1.toByte
    case `rightJoin`    => 2.toByte
    case `fullJoin`     => 3.toByte
    case `leftAntiJoin` => 4.toByte
    case `leftSemiJoin` => 5.toByte
  }

  /** Decode a byte tag back to a joinType. */
  def fromTag(tag: Byte): joinType = tag match {
    case 0 => innerJoin
    case 1 => leftJoin
    case 2 => rightJoin
    case 3 => fullJoin
    case 4 => leftAntiJoin
    case 5 => leftSemiJoin
    case _ => throw new IllegalArgumentException(s"Unknown joinType tag: $tag")
  }
}

case class indexWrapperInt(
    index: Array[Int],
    bloom: BloomFilter,
    lastUpdated: java.sql.Timestamp
) extends indexWrapper


case class indexWrapperLong(
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

/**
  * Mutable tracker that tracks the id's for CDWindexes for quick recall and utilization.
  *
  * @param name         human-readable identifier for this tracker
  * @param trackedIds   mutable collection of row IDs currently being tracked
  * @param wrapper      reference to the underlying index data (sorted array + bloom filter)
  * @param createdAt    immutable creation timestamp
  * @param lastModified mutable timestamp, updated on every mutation
  */
case class IndexTracker(
    name: String,
    trackedIds: ArrayBuffer[Long],
    wrapper: CDWindex,
    createdAt: java.sql.Timestamp,
    var lastModified: java.sql.Timestamp
) {

  /** Add a row ID and update the modification timestamp. */
  def addId(id: Long): Unit = { trackedIds += id; touch() }

  /** Remove a row ID and update the modification timestamp. */
  def removeId(id: Long): Unit = { trackedIds -= id; touch() }

  /** Check whether a row ID is currently tracked. */
  def containsId(id: Long): Boolean = trackedIds.contains(id)

  /** Number of currently tracked IDs. */
  def size: Int = trackedIds.length

  /** Max ID for incrementing ID generation */
  def maxId: Long = if (trackedIds.isEmpty) 0L else trackedIds.max

  /** Getter for the underlying CDWindex */
  def 

  /** Update lastModified to now. */
  def touch(): Unit = {
    lastModified = new java.sql.Timestamp(System.currentTimeMillis())
  }
}

