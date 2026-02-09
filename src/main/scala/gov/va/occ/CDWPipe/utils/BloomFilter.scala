package gov.va.occ.CDWPipe.utils

import java.lang.Math

import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3

/**
  * Bloom filter (probabilistic set membership).
  *
  * Properties:
  * - False positives: possible.
  * - False negatives: not possible *as long as* you only add items and you query
  *   with the same hashing scheme/parameters, and the filter contains all items
  *   that might be queried.
  * - Deletions are not supported (use a counting Bloom filter if you need that).
  *
  * This implementation is optimized for Int/Long membership tests.
  */
final class BloomFilter private (
    private val bits: Array[Long],
    val numBits: Int,
    val numHashes: Int
) {
  require(numBits > 0, "numBits must be > 0")
  require(numHashes > 0, "numHashes must be > 0")

  @inline private def setBit(bitIndex: Int): Unit = {
    val wordIndex = bitIndex >>> 6
    val mask = 1L << (bitIndex & 63)
    bits(wordIndex) |= mask
  }

  @inline private def getBit(bitIndex: Int): Boolean = {
    val wordIndex = bitIndex >>> 6
    val mask = 1L << (bitIndex & 63)
    (bits(wordIndex) & mask) != 0L
  }

  @inline private def mix1(x: Int): Int = {
    // Murmur-inspired; uses Scala's MurmurHash3 utilities.
    MurmurHash3.finalizeHash(MurmurHash3.mix(0x9e3779b9, x), 1)
  }

  @inline private def mix2(x: Int): Int = {
    MurmurHash3.finalizeHash(MurmurHash3.mix(0x85ebca6b, x), 1)
  }

  @inline private def h1h2Int(value: Int): (Int, Int) = {
    val h1 = mix1(value)
    val h2raw = mix2(value)
    // Ensure h2 is not 0 to avoid generating the same index repeatedly.
    val h2 = if (h2raw == 0) 0x27d4eb2d else h2raw
    (h1, h2)
  }

  @inline private def h1h2Long(value: Long): (Int, Int) = {
    // Fold 64-bit to 32-bit in a stable way.
    val lo = value.toInt
    val hi = (value >>> 32).toInt
    val folded = MurmurHash3.mix(lo, hi)
    val h1 = mix1(folded)
    val h2raw = mix2(folded)
    val h2 = if (h2raw == 0) 0x27d4eb2d else h2raw
    (h1, h2)
  }

  /** Add an Int value to the filter. */
  def addInt(value: Int): Unit = {
    val (h1, h2) = h1h2Int(value)
    var i = 0
    while (i < numHashes) {
      val bitIndex = Math.floorMod(h1 + i * h2, numBits)
      setBit(bitIndex)
      i += 1
    }
  }

  /** Returns true if the value might be in the set; false means definitely not in the set. */
  def mightContainInt(value: Int): Boolean = {
    val (h1, h2) = h1h2Int(value)
    var i = 0
    while (i < numHashes) {
      val bitIndex = Math.floorMod(h1 + i * h2, numBits)
      if (!getBit(bitIndex)) return false
      i += 1
    }
    true
  }

  /** Add a Long value to the filter. */
  def addLong(value: Long): Unit = {
    val (h1, h2) = h1h2Long(value)
    var i = 0
    while (i < numHashes) {
      val bitIndex = Math.floorMod(h1 + i * h2, numBits)
      setBit(bitIndex)
      i += 1
    }
  }

  /** Returns true if the value might be in the set; false means definitely not in the set. */
  def mightContainLong(value: Long): Boolean = {
    val (h1, h2) = h1h2Long(value)
    var i = 0
    while (i < numHashes) {
      val bitIndex = Math.floorMod(h1 + i * h2, numBits)
      if (!getBit(bitIndex)) return false
      i += 1
    }
    true
  }

  /** Write this filter's state to a [[java.io.DataOutputStream]] for compact binary serialization. */
  def writeTo(dos: java.io.DataOutputStream): Unit = {
    dos.writeInt(numBits)
    dos.writeInt(numHashes)
    dos.writeInt(bits.length)
    var i = 0
    while (i < bits.length) {
      dos.writeLong(bits(i))
      i += 1
    }
  }

  /** Serialize to a self-contained byte array. */
  def toBytes: Array[Byte] = {
    val baos = new java.io.ByteArrayOutputStream(12 + bits.length * 8)
    val dos = new java.io.DataOutputStream(baos)
    writeTo(dos)
    dos.flush()
    baos.toByteArray
  }
}

object BloomFilter {
  private val Ln2 = Math.log(2.0)

  /**
    * Create a Bloom filter sized for `expectedInsertions` with target false-positive probability `fpp`.
    *
    * Typical fpp values:
    * - 1e-2 (1%) smaller
    * - 1e-3 (0.1%) moderate
    * - 1e-6 very low, larger memory
    */
  def create(expectedInsertions: Int, fpp: Double = 1e-3): BloomFilter = {
    require(expectedInsertions >= 0, "expectedInsertions must be >= 0")
    require(fpp > 0.0 && fpp < 1.0, "fpp must be in (0, 1)")

    if (expectedInsertions == 0) {
      val numBits = 64
      val numHashes = 1
      return new BloomFilter(new Array[Long](1), numBits, numHashes)
    }

    // m = -n ln(p) / (ln 2)^2
    val m = Math.ceil((-expectedInsertions.toDouble * Math.log(fpp)) / (Ln2 * Ln2)).toInt
    val numBits = Math.max(64, m)
    // k = (m/n) ln 2
    val k = Math.max(1, Math.round((numBits.toDouble / expectedInsertions.toDouble) * Ln2).toInt)

    val words = (numBits + 63) >>> 6
    new BloomFilter(new Array[Long](words), numBits, k)
  }

  /** Build a Bloom filter from an Int Array snapshot. */
  def fromInts(buf: Array[Int], fpp: Double = 1e-3): BloomFilter = {
    val filter = create(buf.length, fpp)
    var i = 0
    val n = buf.length
    while (i < n) {
      filter.addInt(buf(i))
      i += 1
    }
    filter
  }

  /** Build a Bloom filter from a Long Array snapshot. */
  def fromLongs(buf: Array[Long], fpp: Double = 1e-3): BloomFilter = {
    val filter = create(buf.length, fpp)
    var i = 0
    val n = buf.length
    while (i < n) {
      filter.addLong(buf(i))
      i += 1
    }
    filter
  }

  /** Reconstruct a BloomFilter from a [[java.io.DataInputStream]] (written by [[BloomFilter.writeTo]]). */
  def readFrom(dis: java.io.DataInputStream): BloomFilter = {
    val numBits = dis.readInt()
    val numHashes = dis.readInt()
    val bitsLen = dis.readInt()
    val bits = new Array[Long](bitsLen)
    var i = 0
    while (i < bitsLen) {
      bits(i) = dis.readLong()
      i += 1
    }
    new BloomFilter(bits, numBits, numHashes)
  }

  /** Reconstruct from a byte array (written by [[BloomFilter.toBytes]]). */
  def fromBytes(bytes: Array[Byte]): BloomFilter = {
    readFrom(new java.io.DataInputStream(new java.io.ByteArrayInputStream(bytes)))
  }
}
