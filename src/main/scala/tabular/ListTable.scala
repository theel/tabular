package tabular

import tabular.QuerySupport.{AliasSelect, AndFilterFunc, NamedSelect}
import tabular.Tabular._


class ListTable[T](data: Seq[T], fac: DataFactory[T]) extends Table[T](fac) {
  type RowkeyFunc = (Row) => Row
  type AggregateFunc = Seq[Row] => Row

  class IndicesRowkeyFunc(indicies: Seq[Int]) extends RowkeyFunc {
    override def apply(v1: Row): Row = {
      indicies.map(v1(_))
    }
  }

  class RowDataFactory(fieldNames: Seq[String]) extends DataFactory[Row] {

    val columns = fieldNames.zipWithIndex.map { case (name, index) => new Column[Row, Any](name, row => row(index))}

    override def getColumns(): Seq[Column[Row, _]] = columns

    override def getValue(value: Row, s: String): Any = ???
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
    val filteredData = if (spec.filters != null) {
      data.filter(new AndFilterFunc[T](spec.filters.asInstanceOf[Seq[FilterFunc[T]]]))
    } else {
      data
    }
    val selectFieldNames: List[String] = spec.selects.zipWithIndex.map {
      case (value, index) => value match {
        case sym: NamedSelect[T] =>
          sym.name
        case alias: AliasSelect[T] =>
          alias.name
        case default =>
          "field%d".format(index)
      }
    }.toList
    val selects = spec.selects.asInstanceOf[Seq[SelectFunc[T]]]
    val selectedGroupedData = if (spec.groupbys != null) {
      //first select
      val selectedData = filteredData.map(t => selects.map(_.apply(t)))

      //the group by
      val groupIndices: Seq[Int] = getIndices(selectFieldNames, spec.groupbys)
      val groupData = selectedData.groupBy(new IndicesRowkeyFunc(groupIndices))
      val aggregatedData = groupData.map(t => aggregate(t._2))
      aggregatedData
      //TODO: implement having
    } else {
      val selectedData = filteredData.map(t => selects.map(_.apply(t)))
      selectedData
    }
    //TODO: ordering, and limit
    val ordered = if (spec.orderbys != null) {
      val sortIndices = getIndices(selectFieldNames, spec.orderbys)
      val sortFunc = (a: Row, b: Row) => {
        var lt = false
        var i = 0
        val n=sortIndices.size
        while (!lt && i < n){
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
      selectedGroupedData.toSeq.sortWith {
        sortFunc
      }
    } else {
      selectedGroupedData
    }

    //TODO: WIP regarding execution step
    val step1 = new IdentityStep[Row](new ListView[Row](ordered.toSeq, new RowDataFactory(selectFieldNames)))
    val step2 = new ExecutionStep[Row, Row]("Step2", step1, _.asInstanceOf[View[Row]])
    new Query(spec, new Execution(step2))
  }

  def getIndices(fieldNames: List[String], names: Seq[Symbol]): Seq[Int] = {
    val indices = names.map(symbol => fieldNames.indexOf(symbol.name))
    val notFoundIndices = indices.zipWithIndex.filter(_._1 == -1).map(_._2) //find anything that doesn't have mapping in select
    if (notFoundIndices.size > 0) {
      val fieldNames = notFoundIndices.map(names(_))
      throw new IllegalArgumentException("Unknown fields %s".format(fieldNames.mkString(",")))
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