package com.cor.graphx.util

/**
 * Object Utils
 */
object Utils {
  /**
   * Method hIndex
   * Calculates h index from a list
   */
  def hIndex(citations: List[Long]): Long = {
    val citationsSorted: List[Long] = citations.sorted
    var result = 0L
    var i = 0
    for (i <- 0 to citationsSorted.length - 1) {
      val smaller = Math.min(citationsSorted(i), citationsSorted.length - i)
      result = Math.max(result, smaller)
    }

    return result
  }
}