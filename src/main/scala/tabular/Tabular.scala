package tabular

import tabular.Tabular._

/**
 * Created by tiong on 5/31/14.
 */

class Query[T](table: Table[T]) {
  var limit: Limit = null
  var having: FilterFunc[T] = null
  var orderbys: Seq[OrderFunc[T]] = null
  var selects: Seq[SelectFunc[T]] = null
  var filter: FilterFunc[T] = null
  var groupbys: Seq[GroupFunc[T]] = null

  def compile(): Execution = table.compile(this)
}

class QueryWrapper[T](query: Query[T]) {
  def compile(): Execution = query.compile()
}

class QueryClause[T](query: Query[T]) extends QueryWrapper[T](query: Query[T]) {
  def groupBy(groupbys: GroupFunc[T]*): QueryClause[T] = {
    query.groupbys = groupbys
    return this
  }

  def orderBy(orderbys: OrderFunc[T]*): QueryClause[T] = {
    query.orderbys = orderbys
    return this
  }

  def having(having: FilterFunc[T]): QueryClause[T] = {
    query.having = having
    return this
  }

  def limit(limit: Limit): QueryClause[T] = {
    query.limit = limit
    return this
  }
}

class Selected[T](query: Query[T], selects: Seq[SelectFunc[T]]) extends QueryClause[T](query) {
  query.selects = selects

  def where(filter: FilterFunc[T]): QueryClause[T] = {
    query.filter = filter
    new QueryClause(query)
  }

}

abstract class View[Row] extends Table[Row] {
}

abstract class Table[T] {
  def select(selects: SelectFunc[T]*): Selected[T] = {
    new Selected[T](new Query(this), selects)
  }

  //abstracts
  def rows(): Iterator[T]

  def compile(query: Query[T]): Execution
}

class Execution(val steps: ExecutionStep[_, View[Row]]) {
  def execute(): View[Row] = steps.execute()
}

class ListTable[T](data: Seq[T]) extends Table[T] {


  override def compile(query: Query[T]): Execution = {
    val identity = new IdentityStep[Seq[T]](data)
    val filteredStep =
      if (query.filter != null) {
        new ExecutionStep[Seq[T], Seq[T]]("Filter with %s".format(query.filter), identity, _.filter(query.filter))
      } else {
        identity
      }
    if (query.groupbys != null) {
      //project group->select tuples
      val projectGroupStep = new ExecutionStep[Seq[T], Seq[RowTuple]]("Project groups (%s) -> (%s)".format(query.groupbys.mkString(","), query.selects.mkString(",")),
        filteredStep, _.map(row => (query.groupbys.map(func => func.apply(row)) ->
          query.selects.map(func => func.apply(row)))))
      val groupByStep = new ExecutionStep[Seq[RowTuple], Map[Row, Seq[RowTuple]]]("Group by group keys",
        projectGroupStep, _.groupBy(_._1))
      val finalStep = new ExecutionStep[Map[Row, Seq[RowTuple]], View[Row]]("Aggregated rows", groupByStep,
        groups =>
        new ListView(groups.map(a => aggregate(a._2.map(_._2))).toSeq))
      new Execution(finalStep)
    } else {
      val finalStep = new ExecutionStep[Seq[T], View[Row]]("Select " + query.selects, filteredStep,
        groups => new ListView(groups.map(row => query.selects.map(func => func.apply(row)))))
      return new Execution(finalStep)
    }
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

class ListView(data: Seq[Row]) extends View[Row] {
  val impl = new ListTable(data)

  //abstracts
  override def rows(): Iterator[Row] = impl.rows()

  override def compile(query: Query[Row]): Execution = impl.compile(query)
}

class NamedSelect[T](name: String) extends SelectFunc[T] {
  override def apply(v1: T): Any = ???
}

class FuncSelect[T](f: T => Any) extends SelectFunc[T] {
  override def apply(v1: T): Any = ???
}


class DataFactory[T] {
  def getValue(value: T, s: String): Any = ???
}

case class Aggregate[T](data: T, f: (T, T) => T) {
  def aggregate(that: Aggregate[T]): Aggregate[T] = {
    new Aggregate(f.apply(data, that.data), f)
  }
}

abstract class LazyStep[That] {
  def execute(): That
}

class IdentityStep[That](that: That) extends LazyStep[That] {
  def execute(): That = {
    that
  }
}

class ExecutionStep[This, That](val desc: String, prev: LazyStep[This], f: (This) => That) extends LazyStep[That] {
  def execute(): That = {
    f.apply(prev.execute())
  }
}


object Tabular {
  type SelectFunc[T] = (T) => Any
  type FilterFunc[T] = (T) => Boolean
  type GroupFunc[T] = (T) => Any
  type OrderFunc[T] = (T) => Any
  type Row = Seq[Any]
  type RowTuple = (Row, Row)
  type Limit = (Int, Int)

  //support select ('firstName)
  class SymbolSelect[T](name: String) extends SelectFunc[T] {
    override def apply(v1: T): Any = ???
  }

  implicit def symbolSelect[T](s: Symbol): SelectFunc[T] = s.name match {
    //    case "*" => new AllSelect[T]()
    case _ => new NamedSelect[T](s.name)
  }


  //support select ("any string", 1, 0.1)
  class LiteralSelect[T](v: AnyVal) extends SelectFunc[T] {
    override def apply(v1: T): Any = v
  }

  implicit def anyValSelect[T](v: AnyVal): SelectFunc[T] = new LiteralSelect[T](v)

  //support select (sum(_.age))
  class SumSelect[T](f: SelectFunc[T]) extends SelectFunc[T] {
    override def apply(v1: T): Any = {
      val value = f.apply(v1)
      new Aggregate[Int](value.asInstanceOf[Int], (a, b) => a + b)
    }
  }

  def sum[T](f: SelectFunc[T]) = {
    new SumSelect[T](f)
  }

}
