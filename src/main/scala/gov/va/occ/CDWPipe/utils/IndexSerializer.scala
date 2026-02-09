package gov.va.occ.CDWPipe.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import scala.collection.mutable.ArrayBuffer

import gov.va.occ.CDWPipe.structs.index._

/**
  * Compact binary codec for [[IndexTracker]] and [[indexWrapper]].
  *
  * Wire format (big-endian, produced by [[java.io.DataOutputStream]]):
  * {{{
  *   4 bytes  – magic number  0x43445749 ("CDWI")
  *   2 bytes  – format version (currently 1)
  *   UTF      – name
  *   UTF      – leftTable
  *   UTF      – rightTable
  *   1 byte   – joinType tag  (see joinType.toTag)
  *   UTF      – indexType ("Int" | "Long")
  *   4 bytes  – trackedIds length (n)
  *   n×8 bytes – trackedIds values (Long)
  *   8 bytes  – createdAt   millis since epoch
  *   8 bytes  – lastModified millis since epoch
  *   --- indexWrapper payload ---
  *   1 byte   – wrapper type tag (0 = Int, 1 = Long)
  *   4|8 bytes – wrapper id  (Int for type 0, Long for type 1)
  *   8 bytes  – wrapper lastUpdated millis
  *   4 bytes  – index array length (m)
  *   m×4|8 bytes – index values
  *   [BloomFilter bytes via BloomFilter.writeTo]
  * }}}
  */
object IndexSerializer {

  private val Magic: Int   = 0x43445749 // "CDWI"
  private val Version: Short = 1

  // ───────── Public API ─────────

  /** Serialize an [[IndexTracker]] to a byte array. */
  def serialize(tracker: IndexTracker): Array[Byte] = {
    val baos = new ByteArrayOutputStream(256)
    val dos  = new DataOutputStream(baos)
    writeTo(dos, tracker)
    dos.flush()
    baos.toByteArray
  }

  /** Deserialize an [[IndexTracker]] from a byte array. */
  def deserialize(bytes: Array[Byte]): IndexTracker = {
    val dis = new DataInputStream(new ByteArrayInputStream(bytes))
    readFrom(dis)
  }

  /** Write an [[IndexTracker]] to a stream. */
  def writeTo(dos: DataOutputStream, tracker: IndexTracker): Unit = {
    // Header
    dos.writeInt(Magic)
    dos.writeShort(Version)

    // Metadata strings
    dos.writeUTF(tracker.name)
    dos.writeUTF(tracker.leftTable)
    dos.writeUTF(tracker.rightTable)
    dos.writeByte(joinType.toTag(tracker.joinType))
    dos.writeUTF(tracker.indexType)

    // Tracked IDs
    val ids = tracker.trackedIds
    dos.writeInt(ids.length)
    var i = 0
    while (i < ids.length) {
      dos.writeLong(ids(i))
      i += 1
    }

    // Timestamps
    dos.writeLong(tracker.createdAt.getTime)
    dos.writeLong(tracker.lastModified.getTime)

    // indexWrapper payload
    writeWrapper(dos, tracker.wrapper)
  }

  /** Read an [[IndexTracker]] from a stream. */
  def readFrom(dis: DataInputStream): IndexTracker = {
    // Header
    val magic = dis.readInt()
    if (magic != Magic)
      throw new IllegalArgumentException(f"Bad magic number: 0x$magic%08X (expected 0x$Magic%08X)")
    val version = dis.readShort()
    if (version != Version)
      throw new IllegalArgumentException(s"Unsupported format version: $version (expected $Version)")

    // Metadata
    val name       = dis.readUTF()
    val leftTable  = dis.readUTF()
    val rightTable = dis.readUTF()
    val jt         = joinType.fromTag(dis.readByte())
    val indexType   = dis.readUTF()

    // Tracked IDs
    val idCount = dis.readInt()
    val ids     = new ArrayBuffer[Long](idCount)
    var i = 0
    while (i < idCount) {
      ids += dis.readLong()
      i += 1
    }

    // Timestamps
    val createdAt    = new java.sql.Timestamp(dis.readLong())
    val lastModified = new java.sql.Timestamp(dis.readLong())

    // indexWrapper
    val wrapper = readWrapper(dis)

    IndexTracker(name, leftTable, rightTable, jt, indexType, ids, wrapper, createdAt, lastModified)
  }

  // ───────── indexWrapper helpers ─────────

  /** Serialize just an [[indexWrapper]] to bytes (useful for standalone persistence). */
  def serializeWrapper(wrapper: indexWrapper): Array[Byte] = {
    val baos = new ByteArrayOutputStream(128)
    val dos  = new DataOutputStream(baos)
    writeWrapper(dos, wrapper)
    dos.flush()
    baos.toByteArray
  }

  /** Deserialize just an [[indexWrapper]] from bytes. */
  def deserializeWrapper(bytes: Array[Byte]): indexWrapper = {
    readWrapper(new DataInputStream(new ByteArrayInputStream(bytes)))
  }

  private def writeWrapper(dos: DataOutputStream, wrapper: indexWrapper): Unit = wrapper match {
    case w: indexWrapperInt =>
      dos.writeByte(0) // type tag: Int
      dos.writeInt(w.id)
      dos.writeLong(w.lastUpdated.getTime)
      dos.writeInt(w.index.length)
      var i = 0
      while (i < w.index.length) {
        dos.writeInt(w.index(i))
        i += 1
      }
      w.bloom.writeTo(dos)

    case w: indexWrapperLong =>
      dos.writeByte(1) // type tag: Long
      dos.writeLong(w.id)
      dos.writeLong(w.lastUpdated.getTime)
      dos.writeInt(w.index.length)
      var i = 0
      while (i < w.index.length) {
        dos.writeLong(w.index(i))
        i += 1
      }
      w.bloom.writeTo(dos)
  }

  private def readWrapper(dis: DataInputStream): indexWrapper = {
    val typeTag = dis.readByte()
    typeTag match {
      case 0 => // Int
        val id          = dis.readInt()
        val lastUpdated = new java.sql.Timestamp(dis.readLong())
        val arrLen      = dis.readInt()
        val arr         = new Array[Int](arrLen)
        var i = 0
        while (i < arrLen) {
          arr(i) = dis.readInt()
          i += 1
        }
        val bloom = BloomFilter.readFrom(dis)
        indexWrapperInt(id, arr, bloom, lastUpdated)

      case 1 => // Long
        val id          = dis.readLong()
        val lastUpdated = new java.sql.Timestamp(dis.readLong())
        val arrLen      = dis.readInt()
        val arr         = new Array[Long](arrLen)
        var i = 0
        while (i < arrLen) {
          arr(i) = dis.readLong()
          i += 1
        }
        val bloom = BloomFilter.readFrom(dis)
        indexWrapperLong(id, arr, bloom, lastUpdated)

      case _ =>
        throw new IllegalArgumentException(s"Unknown indexWrapper type tag: $typeTag")
    }
  }
}
