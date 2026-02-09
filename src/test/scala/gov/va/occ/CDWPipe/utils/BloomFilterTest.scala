package gov.va.occ.CDWPipe.utils

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer

class BloomFilterTest extends AnyFunSuite {
  test("BloomFilter has no false negatives for inserted Ints") {
    val buf = Array.from(0 until 5000)
    val bloom = BloomFilter.fromInts(buf.toArray, fpp = 1e-3)

    var i = 0
    while (i < buf.length) {
      assert(bloom.mightContainInt(buf(i)))
      i += 1
    }
  }

  test("BloomFilter reduces misses (typically) for absent Ints") {
    val buf = Array.from(0 until 5000)
    val bloom = BloomFilter.fromInts(buf.toArray, fpp = 1e-2)

    // False positives are possible, so we can't assert 0.
    // But with fpp=1%, it should be far below a loose bound.
    val absent = 10000 until 11000
    val positives = absent.count(bloom.mightContainInt)
    assert(positives < 200)
  }

  test("BinarySearch + Bloom never misses inserted values") {
    val buf = Array.from(0 until 10000)
    val bloom = BloomFilter.fromInts(buf.toArray, fpp = 1e-3)

    assert(BinarySearch.indexOfSortedIntWithBloom(buf, 1234, bloom) == 1234)
    assert(BinarySearch.indexOfSortedIntWithBloom(buf, 9999, bloom) == 9999)
    assert(BinarySearch.indexOfSortedIntWithBloom(buf, -1, bloom) == -1)
  }
}
