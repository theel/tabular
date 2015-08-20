package tabular.util

import tabular.core.Tabular.Row

/**
 * Created by tiong on 8/19/15.
 */
class RowSorter(indices: Seq[Int]) extends ((Row, Row) => Boolean) {
  override def apply(a: Row, b: Row): Boolean = {
    var lt = false
    var i = 0
    val n = indices.size
    while (!lt && i < n) {
      val aValue = a(i)
      val bValue = b(i)
      val compared = aValue match {
        case compA: Comparable[AnyRef] =>
          compA.compareTo(bValue.asInstanceOf[AnyRef])
        case orderA: Ordered[AnyRef] =>
          orderA.compareTo(bValue.asInstanceOf[AnyRef])
        case default =>
          throw new IllegalArgumentException("Unknown sorting value " + aValue + " and " + bValue)
      }
      lt = compared < 0
      i += 1
    }
    lt

  }
}


