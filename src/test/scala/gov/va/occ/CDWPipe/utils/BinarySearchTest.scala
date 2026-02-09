package gov.va.occ.CDWPipe.utils

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer

class BinarySearchTest extends AnyFunSuite {
  test("indexOfSortedInt finds existing values") {
    val buf = Array(1, 3, 5, 7, 9, 11)
    assert(BinarySearch.indexOfSortedInt(buf, 1) == 0)
    assert(BinarySearch.indexOfSortedInt(buf, 7) == 3)
    assert(BinarySearch.indexOfSortedInt(buf, 11) == 5)
  }

  test("indexOfSortedInt returns -1 when missing") {
    val buf = Array(2, 4, 6, 8, 10)
    assert(BinarySearch.indexOfSortedInt(buf, 1) == -1)
    assert(BinarySearch.indexOfSortedInt(buf, 7) == -1)
    assert(BinarySearch.indexOfSortedInt(buf, 99) == -1)
  }

  test("binarySearchInt insertion point semantics") {
    val buf = Array(10, 20, 30, 40)
    assert(BinarySearch.binarySearchInt(buf, 10) == 0)
    assert(BinarySearch.binarySearchInt(buf, 25) == -3) // insertion point 2 => -(2+1)
    assert(BinarySearch.binarySearchInt(buf, 5) == -1) // insertion point 0
    assert(BinarySearch.binarySearchInt(buf, 50) == -5) // insertion point 4
  }

  test("lowerBoundInt/upperBoundInt behave with duplicates") {
    val buf = Array(1, 2, 2, 2, 3, 5)
    assert(BinarySearch.lowerBoundInt(buf, 2) == 1)
    assert(BinarySearch.upperBoundInt(buf, 2) == 4)
    assert(BinarySearch.lowerBoundInt(buf, 4) == 5)
    assert(BinarySearch.upperBoundInt(buf, 5) == 6)
  }

  test("indexOfExponentialInt matches binary results") {
    val buf = Array.from(0 until 10000)
    assert(BinarySearch.indexOfExponentialInt(buf, 0) == 0)
    assert(BinarySearch.indexOfExponentialInt(buf, 9999) == 9999)
    assert(BinarySearch.indexOfExponentialInt(buf, 1234) == 1234)
    assert(BinarySearch.indexOfExponentialInt(buf, 10001) == -1)
  }
}
