package tabular

import tabular.core.QuerySupport.AndFilterFunc
import tabular.core.Tabular._
import tabular.core._
import tabular.util.RowSorter

/**
 * A list implementation of tabular framework. The table wraps a list of T and allow executing sql query to it
 * @param data a list of T
 * @param fac the data factory for T
 * @tparam T the type
 */
class ListTable[T](data: Seq[T], fac: DataFactory[T]) extends Table[T](fac) {

  type RowkeyFunc = (Row) => Row
  type AggregateFunc = Seq[Row] => Row

  class IndicesRowkeyFunc(indicies: Seq[Int]) extends RowkeyFunc {
    override def apply(v1: Row): Row = {
      indicies.map(v1(_))
    }
  }

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


  override def compile(stmt: Statement[T]): Query[T] = {
    val spec = stmt.spec

    //step 1 - filter
    val filteredData = if (spec.filters != null) {
      data.filter(new AndFilterFunc[T](spec.filters.asInstanceOf[Seq[FilterFunc[T]]]))
    } else {
      data
    }

    //step 2 - project
    val selectedData = filteredData.map(t => spec.selects.map(_.apply(t)))
    val selectFieldNames = spec.getSelectFieldNames

    //step 3 - group
    val selectedGroupedData = if (spec.groupbys != null) {
      //the group by
      val groupIndices: Seq[Int] = getIndices(selectFieldNames, spec.groupbys)
      val groupData = selectedData.groupBy(new IndicesRowkeyFunc(groupIndices))
      val aggregatedData = groupData.map(t => aggregate(t._2))
      aggregatedData
      //TODO: implement having
    } else {
      selectedData
    }
    //TODO: ordering, and limit
    val ordered = if (spec.orderbys != null) {
      val sortIndices = getIndices(selectFieldNames, spec.orderbys)
      selectedGroupedData.toSeq.sortWith {
        new RowSorter(sortIndices)
      }
    } else {
      selectedGroupedData
    }

    //TODO: WIP regarding execution step
    val step1 = new IdentityStep[Row](new ListView[Row](ordered.toSeq, new RowDataFactory(selectFieldNames.map(_.name))))
    val step2 = new ExecutionStep[Row, Row]("Step2", step1, _.asInstanceOf[View[Row]])
    new Query(spec, new Execution(step2))
  }

  def getIndices(fieldNames: List[Symbol], names: Seq[Symbol]): Seq[Int] = {
    val indices = names.map(symbol => fieldNames.indexOf(symbol))
    val notFoundIndices = indices.zipWithIndex.filter(_._1 == -1).map(_._2) //find anything that doesn't have mapping in select
    if (notFoundIndices.size > 0) {
      val fieldNames = notFoundIndices.map(names(_))
      throw new IllegalArgumentException("Unknown fields %s. Possible fields = %s".format(fieldNames.mkString(","), names.mkString(",")))
    }
    indices
  }

  def aggregate(data: Seq[Row]): Row = {
    val results = data.reduce((a: Row, b: Row) => {
      a.zip(b).map(t => if (classOf[Aggregate[Int]].isInstance(t._1)) {
        t._1.asInstanceOf[Aggregate[Int]].aggregate(t._2.asInstanceOf[Aggregate[Int]])
      } else t._1)
    })
    results.map(a => if (classOf[Aggregate[Int]].isInstance(a)) a.asInstanceOf[Aggregate[Int]].data else a)
  }

  override def rows(): Iterator[T] = data.iterator
}

class ListView[T](data: Seq[T], fac: DataFactory[T]) extends View[T](fac) {
  val impl = new ListTable(data, fac)

  //abstracts
  override def rows(): Iterator[T] = impl.rows()

  override def compile(stmt: Statement[T]): Query[T] = impl.compile(stmt)

  override def toString(): String = {
    val columns = fac.getColumns().map(c => "\"%s\"".format(c.name)).mkString(",")
    "ListView[%s]\n%s".format(columns, impl.rows().toSeq.mkString("\n"))
  }
}