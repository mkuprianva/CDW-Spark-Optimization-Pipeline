package gov.va.occ.CDWPipe.utils

import scala.collection.immutable.ArraySeq
import scala.collection.mutable.ArrayBuffer

/**
  * Sorted-array intersection utilities.
  *
  * Complexity guide (both arrays sorted ascending):
  *
  * 1. Two-pointer merge      — O(n + m), simple, best when sizes are similar.
  * 2. Two-pointer + dedup    — same O(n + m), but skips consecutive duplicates
  *                              using a primitive sentinel instead of Option boxing.
  * 3. Galloping (exponential) — O(m · log(n/m)) where m ≤ n.
  *                              Much faster when one side is dramatically smaller.
  *
  * All methods produce a **sorted, duplicate-free** result.
  */
object ArrayIntersect {

  // ───────────────────── Int variants ─────────────────────

  /**
    * Two-pointer intersection of two sorted Int arrays.
    * Output is sorted and duplicate-free (uses primitive sentinel, no boxing).
    */
  def intersectSortedInt(a: ArraySeq[Int], b: ArraySeq[Int]): ArraySeq[Int] = {
    val aLen = a.length
    val bLen = b.length
    if (aLen == 0 || bLen == 0) return ArraySeq.empty[Int]

    // Fast range-overlap reject.
    if (a(aLen - 1) < b(0) || b(bLen - 1) < a(0)) return ArraySeq.empty[Int]

    val res = new ArrayBuffer[Int](math.min(aLen, bLen))
    var i = 0
    var j = 0
    var last = Int.MinValue  // primitive sentinel — no Option boxing
    var hasLast = false

    while (i < aLen && j < bLen) {
      val x = a(i)
      val y = b(j)
      if (x < y) {
        i += 1
      } else if (x > y) {
        j += 1
      } else {
        // x == y — emit only if it differs from the previous emitted value
        if (!hasLast || last != x) {
          res += x
          last = x
          hasLast = true
        }
        i += 1
        j += 1
      }
    }

    ArraySeq.unsafeWrapArray(res.toArray)
  }

  /**
    * Galloping intersection for sorted Int arrays.
    *
    * When |a| ≪ |b| (or vice-versa), galloping skips large stretches of the
    * bigger array in O(log(n/m)) per element of the smaller array instead of
    * scanning them one-by-one.
    *
    * Always walks the *smaller* side linearly and gallops through the larger.
    */
  def intersectSortedIntGalloping(a: ArraySeq[Int], b: ArraySeq[Int]): ArraySeq[Int] = {
    val aLen = a.length
    val bLen = b.length
    if (aLen == 0 || bLen == 0) return ArraySeq.empty[Int]
    if (a(aLen - 1) < b(0) || b(bLen - 1) < a(0)) return ArraySeq.empty[Int]

    // Ensure `small` is the shorter side.
    val (small, big, sLen, bLenFinal) =
      if (aLen <= bLen) (a, b, aLen, bLen)
      else              (b, a, bLen, aLen)

    val res = new ArrayBuffer[Int](sLen) // upper bound on result size
    var bStart = 0 // sliding lower bound into `big`
    var last = Int.MinValue
    var hasLast = false
    var si = 0

    while (si < sLen && bStart < bLenFinal) {
      val target = small(si)

      // Gallop: exponential search in big[bStart ..] for `target`.
      var bound = 1
      while (bStart + bound < bLenFinal && big(bStart + bound) < target) {
        bound <<= 1
      }

      // Binary search in big[bStart + bound/2 .. min(bStart + bound, bLenFinal-1)]
      var lo = bStart + (bound >>> 1)
      var hi = math.min(bStart + bound, bLenFinal - 1)

      // Standard binary search for `target`
      var found = false
      while (lo <= hi) {
        val mid = lo + ((hi - lo) >>> 1)
        val midVal = big(mid)
        if (midVal < target) lo = mid + 1
        else if (midVal > target) hi = mid - 1
        else { found = true; lo = mid; hi = mid - 1 } // keep leftmost
      }

      if (found) {
        if (!hasLast || last != target) {
          res += target
          last = target
          hasLast = true
        }
        bStart = lo + 1 // advance past the match
      } else {
        bStart = lo // lo is the first element >= target (and > target since not found)
      }

      si += 1
    }

    ArraySeq.unsafeWrapArray(res.toArray)
  }

  /**
    * Auto-dispatch: uses galloping when one side is ≥ 8× larger, otherwise
    * plain two-pointer.  Both paths produce sorted, deduplicated output.
    */
  def intersectInt(a: ArraySeq[Int], b: ArraySeq[Int]): ArraySeq[Int] = {
    val ratio = if (a.length > 0 && b.length > 0) {
      math.max(a.length, b.length).toDouble / math.min(a.length, b.length)
    } else 0.0

    if (ratio >= 8.0) intersectSortedIntGalloping(a, b)
    else               intersectSortedInt(a, b)
  }

  // ───────────────────── Long variants ─────────────────────

  /** Two-pointer intersection for sorted Long arrays (deduped). */
  def intersectSortedLong(a: ArraySeq[Long], b: ArraySeq[Long]): ArraySeq[Long] = {
    val aLen = a.length
    val bLen = b.length
    if (aLen == 0 || bLen == 0) return ArraySeq.empty[Long]
    if (a(aLen - 1) < b(0) || b(bLen - 1) < a(0)) return ArraySeq.empty[Long]

    val res = new ArrayBuffer[Long](math.min(aLen, bLen))
    var i = 0
    var j = 0
    var last = Long.MinValue
    var hasLast = false

    while (i < aLen && j < bLen) {
      val x = a(i)
      val y = b(j)
      if (x < y) {
        i += 1
      } else if (x > y) {
        j += 1
      } else {
        if (!hasLast || last != x) {
          res += x
          last = x
          hasLast = true
        }
        i += 1
        j += 1
      }
    }

    ArraySeq.unsafeWrapArray(res.toArray)
  }

  /** Galloping intersection for sorted Long arrays (deduped). */
  def intersectSortedLongGalloping(a: ArraySeq[Long], b: ArraySeq[Long]): ArraySeq[Long] = {
    val aLen = a.length
    val bLen = b.length
    if (aLen == 0 || bLen == 0) return ArraySeq.empty[Long]
    if (a(aLen - 1) < b(0) || b(bLen - 1) < a(0)) return ArraySeq.empty[Long]

    val (small, big, sLen, bLenFinal) =
      if (aLen <= bLen) (a, b, aLen, bLen)
      else              (b, a, bLen, aLen)

    val res = new ArrayBuffer[Long](sLen)
    var bStart = 0
    var last = Long.MinValue
    var hasLast = false
    var si = 0

    while (si < sLen && bStart < bLenFinal) {
      val target = small(si)

      var bound = 1
      while (bStart + bound < bLenFinal && big(bStart + bound) < target) {
        bound <<= 1
      }

      var lo = bStart + (bound >>> 1)
      var hi = math.min(bStart + bound, bLenFinal - 1)
      var found = false

      while (lo <= hi) {
        val mid = lo + ((hi - lo) >>> 1)
        val midVal = big(mid)
        if (midVal < target) lo = mid + 1
        else if (midVal > target) hi = mid - 1
        else { found = true; lo = mid; hi = mid - 1 }
      }

      if (found) {
        if (!hasLast || last != target) {
          res += target
          last = target
          hasLast = true
        }
        bStart = lo + 1
      } else {
        bStart = lo
      }

      si += 1
    }

    ArraySeq.unsafeWrapArray(res.toArray)
  }

  /** Auto-dispatch for Long. */
  def intersectLong(a: ArraySeq[Long], b: ArraySeq[Long]): ArraySeq[Long] = {
    val ratio = if (a.length > 0 && b.length > 0) {
      math.max(a.length, b.length).toDouble / math.min(a.length, b.length)
    } else 0.0

    if (ratio >= 8.0) intersectSortedLongGalloping(a, b)
    else               intersectSortedLong(a, b)
  }
}
