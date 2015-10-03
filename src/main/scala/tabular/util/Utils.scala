package tabular.util

import tabular.core.Table._

/**
 * Created by tiong on 8/20/15.
 */
object Utils {
  def getIndices(fieldNames: List[Symbol], names: Seq[Symbol]): Seq[Int] = {
    val indices = names.map(symbol => fieldNames.indexOf(symbol))
    val notFoundIndices = indices.zipWithIndex.filter(_._1 == -1).map(_._2) //find anything that doesn't have mapping in select
    if (notFoundIndices.size > 0) {
      val fieldNames = notFoundIndices.map(names(_))
      throw new IllegalArgumentException("Unknown fields %s. Possible fields = %s".format(fieldNames.mkString(","), names.mkString(",")))
    }
    indices
  }


  type RowkeyFunc = (Row) => Row

  case class IndicesRowkeyFunc(indicies: Seq[Int]) extends RowkeyFunc {
    override def apply(v1: Row): Row = {
      indicies.map(v1(_))
    }
  }

  case class RowAggregator extends ((Row, Row) => Row) {
    override def apply(r1: Row, r2: Row): Row = {
      r1.zip(r2).map{
        case (a: Aggregate[Int], b: Aggregate[Int]) =>
          a.aggregate(b)
        case t => t._1
      }
    }
  }

  type AggregateFunc = Seq[Row] => Row

  class SimpleAggregateFunc extends AggregateFunc {
    def apply(data: Seq[Row]): Row = {
      val results = data.reduce((a: Row, b: Row) => {
        a.zip(b).map(t => if (classOf[Aggregate[Int]].isInstance(t._1)) {
          t._1.asInstanceOf[Aggregate[Int]].aggregate(t._2.asInstanceOf[Aggregate[Int]])
        } else t._1)
      })
      results.map(a => if (classOf[Aggregate[Int]].isInstance(a)) a.asInstanceOf[Aggregate[Int]].data else a)
    }
  }

  def normalizeAggregate(row: Row) = {
    row.map{
      case (a: Aggregate[Int]) =>
        a.data
      case b =>
        b
    }
  }
}
