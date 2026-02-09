package gov.va.occ.CDWPipe.utils

import scala.collection.mutable.ArrayBuffer

object BinarySearch {

	/**
		* Search algorithm overview (high level):
		*
		* - Linear search: O(n). Best for very small buffers or unsorted data.
		* - Binary search: O(log n). Best general-purpose for sorted data.
		* - Exponential (galloping) search: O(log p) where p is the position of the match.
		*   Great if values you look for tend to be near the beginning.
		* - Hash-based lookup: ~O(1) average, but needs extra memory and a build step.
		*   Best when you do many repeated lookups on mostly-static data.
		* - Bloom filter pre-check: O(k) bit probes. If it says "definitely not present",
		*   you can skip the real search (guaranteed no false negatives) as long as the
		*   filter was built from (and kept in sync with) the underlying collection.
		*/

	/**
		* For small collections, linear search is often faster than binary search due to
		* lower constant factors and fewer hard-to-predict branches.
		*/
	final val DefaultLinearThreshold: Int = 32

	/**
		* Returns index of `value` in `buf` using linear search, or -1 if not found.
		*
		* Note: uses `==` equality.
		*/
	def indexOfLinear[T](buf: ArrayBuffer[T], value: T): Int = {
		var i = 0
		val n = buf.length
		while (i < n) {
			if (buf(i) == value) return i
			i += 1
		}
		-1
	}

	/**
		* Binary search on an ascending-sorted `ArrayBuffer[Int]`.
		*
		* Java/Scala stdlib convention:
		* - If found: returns index >= 0.
		* - If not found: returns `-(insertionPoint + 1)`.
		*/
	def binarySearchInt(buf: ArrayBuffer[Int], value: Int): Int = {
		var lo = 0
		var hi = buf.length - 1

		while (lo <= hi) {
			val mid = lo + ((hi - lo) >>> 1)
			val midVal = buf(mid)
			if (midVal < value) lo = mid + 1
			else if (midVal > value) hi = mid - 1
			else return mid
		}

		-(lo + 1)
	}

	/** Returns the index of `value` in sorted `buf`, or -1 if not found. */
	def indexOfSortedInt(buf: ArrayBuffer[Int], value: Int, linearThreshold: Int = DefaultLinearThreshold): Int = {
		val n = buf.length
		if (n <= 0) return -1
		if (n <= linearThreshold) return indexOfLinear(buf, value)

		val idx = binarySearchInt(buf, value)
		if (idx >= 0) idx else -1
	}

	/**
		* Returns the first index i such that buf(i) >= value.
		* If all elements are < value, returns buf.length.
		*/
	def lowerBoundInt(buf: ArrayBuffer[Int], value: Int): Int = {
		var lo = 0
		var hi = buf.length
		while (lo < hi) {
			val mid = lo + ((hi - lo) >>> 1)
			if (buf(mid) < value) lo = mid + 1
			else hi = mid
		}
		lo
	}

	/**
		* Returns the first index i such that buf(i) > value.
		* If no elements are > value, returns buf.length.
		*/
	def upperBoundInt(buf: ArrayBuffer[Int], value: Int): Int = {
		var lo = 0
		var hi = buf.length
		while (lo < hi) {
			val mid = lo + ((hi - lo) >>> 1)
			if (buf(mid) <= value) lo = mid + 1
			else hi = mid
		}
		lo
	}

	/**
		* Exponential (a.k.a. galloping) search for ascending-sorted Int buffers.
		* Useful when the sought value is likely near the beginning.
		* Returns index or -1.
		*/
	def indexOfExponentialInt(buf: ArrayBuffer[Int], value: Int, linearThreshold: Int = DefaultLinearThreshold): Int = {
		val n = buf.length
		if (n == 0) return -1
		if (n <= linearThreshold) return indexOfLinear(buf, value)

		// Fast reject using endpoints to avoid extra work.
		if (value < buf(0) || value > buf(n - 1)) return -1
		if (buf(0) == value) return 0

		var bound = 1
		while (bound < n && buf(bound) < value) {
			bound <<= 1
		}

		val lo = bound >>> 1
		val hi = math.min(bound, n - 1)

		// Binary search in [lo, hi]
		var left = lo
		var right = hi
		while (left <= right) {
			val mid = left + ((right - left) >>> 1)
			val midVal = buf(mid)
			if (midVal < value) left = mid + 1
			else if (midVal > value) right = mid - 1
			else return mid
		}

		-1
	}

	/** Binary search on an ascending-sorted `ArrayBuffer[Long]` with insertion-point semantics. */
	def binarySearchLong(buf: ArrayBuffer[Long], value: Long): Int = {
		var lo = 0
		var hi = buf.length - 1
		while (lo <= hi) {
			val mid = lo + ((hi - lo) >>> 1)
			val midVal = buf(mid)
			if (midVal < value) lo = mid + 1
			else if (midVal > value) hi = mid - 1
			else return mid
		}
		-(lo + 1)
	}

	def indexOfSortedLong(buf: ArrayBuffer[Long], value: Long, linearThreshold: Int = DefaultLinearThreshold): Int = {
		val n = buf.length
		if (n == 0) return -1
		if (n <= linearThreshold) {
			var i = 0
			while (i < n) {
				if (buf(i) == value) return i
				i += 1
			}
			return -1
		}

		val idx = binarySearchLong(buf, value)
		if (idx >= 0) idx else -1
	}

	/**
		* Generic binary search for ascending-sorted buffers.
		* Prefer the Int/Long specializations in hot paths.
		*/
	def binarySearch[T](buf: ArrayBuffer[T], value: T)(implicit ord: Ordering[T]): Int = {
		var lo = 0
		var hi = buf.length - 1
		while (lo <= hi) {
			val mid = lo + ((hi - lo) >>> 1)
			val midVal = buf(mid)
			val cmp = ord.compare(midVal, value)
			if (cmp < 0) lo = mid + 1
			else if (cmp > 0) hi = mid - 1
			else return mid
		}
		-(lo + 1)
	}

	def indexOfSorted[T](buf: ArrayBuffer[T], value: T, linearThreshold: Int = DefaultLinearThreshold)(implicit
			ord: Ordering[T]
	): Int = {
		val n = buf.length
		if (n == 0) return -1
		if (n <= linearThreshold) return indexOfLinear(buf, value)
		val idx = binarySearch(buf, value)
		if (idx >= 0) idx else -1
	}

	/**
		* Optional optimization: use a Bloom filter as a fast-reject stage.
		*
		* Contract:
		* - If `bloom.mightContainInt(value)` is false, `value` is definitely not in the set *represented by bloom*.
		* - This yields **no false negatives** only if bloom includes all values in `buf`.
		*   If `buf` changes and bloom isn't updated, you can introduce false negatives.
		*/
	def indexOfSortedIntWithBloom(
			buf: ArrayBuffer[Int],
			value: Int,
			bloom: BloomFilter,
			linearThreshold: Int = DefaultLinearThreshold
	): Int = {
		if (!bloom.mightContainInt(value)) return -1
		indexOfSortedInt(buf, value, linearThreshold)
	}

	/**
		* For unsorted buffers, Bloom + linear search is often a big win when most lookups are misses.
		*/
	def indexOfUnsortedIntWithBloom(buf: ArrayBuffer[Int], value: Int, bloom: BloomFilter): Int = {
		if (!bloom.mightContainInt(value)) return -1
		indexOfLinear(buf, value)
	}
}
