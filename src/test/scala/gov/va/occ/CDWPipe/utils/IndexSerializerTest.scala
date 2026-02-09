package gov.va.occ.CDWPipe.utils

import org.scalatest.funsuite.AnyFunSuite
import scala.collection.mutable.ArrayBuffer

import gov.va.occ.CDWPipe.structs.index._

class IndexSerializerTest extends AnyFunSuite {

  // ───────── BloomFilter round-trip ─────────

  test("BloomFilter toBytes/fromBytes round-trip preserves all state") {
    val original = BloomFilter.fromInts(Array(1, 2, 3, 100, 999), fpp = 1e-3)
    val bytes    = original.toBytes
    val restored = BloomFilter.fromBytes(bytes)

    assert(restored.numBits   === original.numBits)
    assert(restored.numHashes === original.numHashes)
    // Every value inserted should still be detected
    assert(restored.mightContainInt(1))
    assert(restored.mightContainInt(999))
    // A value never inserted should (almost certainly) not be detected
    assert(!restored.mightContainInt(12345))
  }

  test("BloomFilter round-trip with Longs") {
    val original = BloomFilter.fromLongs(Array(10L, 20L, 30L, Long.MaxValue), fpp = 1e-3)
    val restored = BloomFilter.fromBytes(original.toBytes)

    assert(restored.mightContainLong(10L))
    assert(restored.mightContainLong(Long.MaxValue))
    assert(!restored.mightContainLong(9999999L))
  }

  // ───────── indexWrapper round-trip ─────────

  test("indexWrapperInt serialize/deserialize round-trip") {
    val idx   = Array(1, 3, 5, 7, 9)
    val bloom = BloomFilter.fromInts(idx, fpp = 1e-3)
    val ts    = new java.sql.Timestamp(1700000000000L)
    val original = indexWrapperInt(42, idx, bloom, ts)

    val bytes    = IndexSerializer.serializeWrapper(original)
    val restored = IndexSerializer.deserializeWrapper(bytes).asInstanceOf[indexWrapperInt]

    assert(restored.id === 42)
    assert(restored.index.sameElements(idx))
    assert(restored.lastUpdated === ts)
    assert(restored.bloom.numBits   === bloom.numBits)
    assert(restored.bloom.numHashes === bloom.numHashes)
    // Bloom still works
    assert(restored.bloom.mightContainInt(5))
    assert(!restored.bloom.mightContainInt(100))
  }

  test("indexWrapperLong serialize/deserialize round-trip") {
    val idx   = Array(100L, 200L, 300L, Long.MaxValue)
    val bloom = BloomFilter.fromLongs(idx, fpp = 1e-3)
    val ts    = new java.sql.Timestamp(1700000000000L)
    val original = indexWrapperLong(99L, idx, bloom, ts)

    val bytes    = IndexSerializer.serializeWrapper(original)
    val restored = IndexSerializer.deserializeWrapper(bytes).asInstanceOf[indexWrapperLong]

    assert(restored.id === 99L)
    assert(restored.index.sameElements(idx))
    assert(restored.lastUpdated === ts)
    assert(restored.bloom.mightContainLong(Long.MaxValue))
  }

  // ───────── IndexTracker round-trip ─────────

  test("IndexTracker full round-trip with Int wrapper") {
    val idx   = Array(10, 20, 30, 40, 50)
    val bloom = BloomFilter.fromInts(idx, fpp = 1e-3)
    val wrapper = indexWrapperInt(1, idx, bloom, new java.sql.Timestamp(1700000000000L))

    val tracker = IndexTracker(
      name         = "test-tracker",
      leftTable    = "patients",
      rightTable   = "visits",
      joinType     = innerJoin,
      indexType     = "Int",
      trackedIds   = ArrayBuffer(100L, 200L, 300L),
      wrapper      = wrapper,
      createdAt    = new java.sql.Timestamp(1700000000000L),
      lastModified = new java.sql.Timestamp(1700001000000L)
    )

    val bytes    = IndexSerializer.serialize(tracker)
    val restored = IndexSerializer.deserialize(bytes)

    assert(restored.name       === "test-tracker")
    assert(restored.leftTable  === "patients")
    assert(restored.rightTable === "visits")
    assert(restored.joinType   === innerJoin)
    assert(restored.indexType  === "Int")
    assert(restored.trackedIds === ArrayBuffer(100L, 200L, 300L))
    assert(restored.createdAt  === new java.sql.Timestamp(1700000000000L))
    assert(restored.lastModified === new java.sql.Timestamp(1700001000000L))

    val w = restored.wrapper.asInstanceOf[indexWrapperInt]
    assert(w.id === 1)
    assert(w.index.sameElements(idx))
    assert(w.bloom.mightContainInt(30))
  }

  test("IndexTracker full round-trip with Long wrapper") {
    val idx   = Array(1000L, 2000L, 3000L)
    val bloom = BloomFilter.fromLongs(idx, fpp = 1e-2)
    val wrapper = indexWrapperLong(7L, idx, bloom, new java.sql.Timestamp(1700000000000L))

    val tracker = IndexTracker(
      name         = "long-tracker",
      leftTable    = "table_a",
      rightTable   = "table_b",
      joinType     = leftAntiJoin,
      indexType     = "Long",
      trackedIds   = ArrayBuffer(1L, 2L),
      wrapper      = wrapper,
      createdAt    = new java.sql.Timestamp(1700000000000L),
      lastModified = new java.sql.Timestamp(1700002000000L)
    )

    val bytes    = IndexSerializer.serialize(tracker)
    val restored = IndexSerializer.deserialize(bytes)

    assert(restored.joinType  === leftAntiJoin)
    assert(restored.indexType === "Long")
    assert(restored.trackedIds === ArrayBuffer(1L, 2L))

    val w = restored.wrapper.asInstanceOf[indexWrapperLong]
    assert(w.id === 7L)
    assert(w.index.sameElements(idx))
    assert(w.bloom.mightContainLong(2000L))
  }

  // ───────── Edge cases ─────────

  test("IndexTracker round-trip with empty trackedIds and empty index") {
    val idx   = Array.empty[Int]
    val bloom = BloomFilter.create(0)
    val wrapper = indexWrapperInt(0, idx, bloom, new java.sql.Timestamp(0L))

    val tracker = IndexTracker(
      name         = "empty",
      leftTable    = "",
      rightTable   = "",
      joinType     = fullJoin,
      indexType     = "Int",
      trackedIds   = ArrayBuffer.empty[Long],
      wrapper      = wrapper,
      createdAt    = new java.sql.Timestamp(0L),
      lastModified = new java.sql.Timestamp(0L)
    )

    val restored = IndexSerializer.deserialize(IndexSerializer.serialize(tracker))

    assert(restored.name          === "empty")
    assert(restored.trackedIds.isEmpty)
    assert(restored.wrapper.asInstanceOf[indexWrapperInt].index.isEmpty)
  }

  test("bad magic number throws") {
    val badBytes = Array[Byte](0, 0, 0, 0, 0, 1) // wrong magic
    assertThrows[IllegalArgumentException] {
      IndexSerializer.deserialize(badBytes)
    }
  }

  // ───────── joinType tag round-trip ─────────

  test("joinType toTag/fromTag round-trip for all variants") {
    val all = Seq(innerJoin, leftJoin, rightJoin, fullJoin, leftAntiJoin, leftSemiJoin)
    all.foreach { jt =>
      assert(joinType.fromTag(joinType.toTag(jt)) === jt)
    }
  }

  // ───────── IndexTracker mutability ─────────

  test("IndexTracker addId/removeId/containsId") {
    val bloom   = BloomFilter.create(0)
    val wrapper = indexWrapperInt(0, Array.empty[Int], bloom, new java.sql.Timestamp(0L))
    val tracker = IndexTracker("t", "", "", innerJoin, "Int", ArrayBuffer.empty[Long],
      wrapper, new java.sql.Timestamp(0L), new java.sql.Timestamp(0L))

    assert(tracker.size === 0)

    tracker.addId(42L)
    tracker.addId(99L)
    assert(tracker.size === 2)
    assert(tracker.containsId(42L))

    tracker.removeId(42L)
    assert(tracker.size === 1)
    assert(!tracker.containsId(42L))
    assert(tracker.containsId(99L))
  }
}
