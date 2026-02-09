package gov.va.occ.CDWPipe.utils

import org.scalatest.funsuite.AnyFunSuite
import scala.collection.immutable.ArraySeq

class ArrayIntersectTest extends AnyFunSuite {

  // ───────── Int two-pointer ─────────

  test("intersectSortedInt — basic overlap") {
    val a = ArraySeq(1, 2, 3, 4, 5)
    val b = ArraySeq(3, 4, 5, 6, 7)
    assert(ArrayIntersect.intersectSortedInt(a, b) === ArraySeq(3, 4, 5))
  }

  test("intersectSortedInt — identical arrays") {
    val a = ArraySeq(1, 2, 3)
    assert(ArrayIntersect.intersectSortedInt(a, a) === ArraySeq(1, 2, 3))
  }

  test("intersectSortedInt — no overlap (disjoint ranges)") {
    val a = ArraySeq(1, 2, 3)
    val b = ArraySeq(4, 5, 6)
    assert(ArrayIntersect.intersectSortedInt(a, b) === ArraySeq.empty[Int])
  }

  test("intersectSortedInt — empty input") {
    assert(ArrayIntersect.intersectSortedInt(ArraySeq.empty[Int], ArraySeq(1, 2)) === ArraySeq.empty[Int])
    assert(ArrayIntersect.intersectSortedInt(ArraySeq(1, 2), ArraySeq.empty[Int]) === ArraySeq.empty[Int])
  }

  test("intersectSortedInt — duplicates in input are deduplicated") {
    val a = ArraySeq(1, 2, 2, 3, 3, 3)
    val b = ArraySeq(2, 2, 3, 4)
    assert(ArrayIntersect.intersectSortedInt(a, b) === ArraySeq(2, 3))
  }

  test("intersectSortedInt — single element match") {
    assert(ArrayIntersect.intersectSortedInt(ArraySeq(5), ArraySeq(5)) === ArraySeq(5))
  }

  test("intersectSortedInt — single element no match") {
    assert(ArrayIntersect.intersectSortedInt(ArraySeq(5), ArraySeq(6)) === ArraySeq.empty[Int])
  }

  // ───────── Int galloping ─────────

  test("intersectSortedIntGalloping — small vs large array") {
    val small = ArraySeq(10, 500, 9999)
    val big   = ArraySeq.from(0 to 10000)
    assert(ArrayIntersect.intersectSortedIntGalloping(small, big) === ArraySeq(10, 500, 9999))
  }

  test("intersectSortedIntGalloping — big on left, small on right (auto-swap)") {
    val big   = ArraySeq.from(0 to 10000)
    val small = ArraySeq(10, 500, 9999)
    assert(ArrayIntersect.intersectSortedIntGalloping(big, small) === ArraySeq(10, 500, 9999))
  }

  test("intersectSortedIntGalloping — empty result") {
    val small = ArraySeq(10001, 10002)
    val big   = ArraySeq.from(0 to 10000)
    assert(ArrayIntersect.intersectSortedIntGalloping(small, big) === ArraySeq.empty[Int])
  }

  test("intersectSortedIntGalloping — duplicates deduplicated") {
    val small = ArraySeq(5, 5, 5)
    val big   = ArraySeq(1, 2, 3, 4, 5, 5, 6, 7)
    assert(ArrayIntersect.intersectSortedIntGalloping(small, big) === ArraySeq(5))
  }

  // ───────── Int auto-dispatch ─────────

  test("intersectInt — similar sizes uses two-pointer") {
    val a = ArraySeq(1, 3, 5, 7)
    val b = ArraySeq(2, 3, 5, 8)
    assert(ArrayIntersect.intersectInt(a, b) === ArraySeq(3, 5))
  }

  test("intersectInt — skewed sizes uses galloping") {
    val small = ArraySeq(50)
    val big   = ArraySeq.from(0 to 1000) // ratio = 1001 ≫ 8
    assert(ArrayIntersect.intersectInt(small, big) === ArraySeq(50))
  }

  // ───────── Long two-pointer ─────────

  test("intersectSortedLong — basic overlap") {
    val a = ArraySeq(10L, 20L, 30L)
    val b = ArraySeq(20L, 30L, 40L)
    assert(ArrayIntersect.intersectSortedLong(a, b) === ArraySeq(20L, 30L))
  }

  test("intersectSortedLong — no overlap") {
    val a = ArraySeq(1L, 2L)
    val b = ArraySeq(3L, 4L)
    assert(ArrayIntersect.intersectSortedLong(a, b) === ArraySeq.empty[Long])
  }

  test("intersectSortedLong — duplicates deduplicated") {
    val a = ArraySeq(1L, 1L, 2L, 3L)
    val b = ArraySeq(1L, 2L, 2L, 4L)
    assert(ArrayIntersect.intersectSortedLong(a, b) === ArraySeq(1L, 2L))
  }

  // ───────── Long galloping ─────────

  test("intersectSortedLongGalloping — small vs large") {
    val small = ArraySeq(100L, 5000L)
    val big   = ArraySeq.from((0L to 10000L))
    assert(ArrayIntersect.intersectSortedLongGalloping(small, big) === ArraySeq(100L, 5000L))
  }

  test("intersectLong — auto-dispatch similar sizes") {
    val a = ArraySeq(1L, 2L, 3L)
    val b = ArraySeq(2L, 3L, 4L)
    assert(ArrayIntersect.intersectLong(a, b) === ArraySeq(2L, 3L))
  }

  test("intersectLong — auto-dispatch skewed sizes") {
    val small = ArraySeq(500L)
    val big   = ArraySeq.from(0L to 10000L)
    assert(ArrayIntersect.intersectLong(small, big) === ArraySeq(500L))
  }
}
