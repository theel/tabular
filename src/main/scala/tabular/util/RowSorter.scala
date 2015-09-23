package tabular.util

import tabular.core.Tabular.Row

object RowOrdering extends Ordering[Row] {
  override def compare(x: Row, y: Row): Int = {
    var i = 0
    while (i<x.length){
      val (a, b) = (x(i).toString, y(i).toString)
      val value  = a.compare(b)
      if (value!=0){
        return value
      }
    }
    return 0
  }
}
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


