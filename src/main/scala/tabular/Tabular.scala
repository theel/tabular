package tabular

import tabular.Tabular._

/**
 * Represent query state
 * @tparam T
 */
case class QuerySpec[T](val table: Table[T]) extends Cloneable {
  var limit: Limit = null
  var having: FilterFunc[T] = null
  var orderbys: Seq[OrderFunc[T]] = null
  var selects: Seq[SelectFunc[T]] = null
  var filters: Seq[FilterFunc[T]] = null
  var groupbys: Seq[GroupFunc[T]] = null
}

/**
 * Represent a query
 * @tparam T
 */
class Query[T](val spec: QuerySpec[T], val exec: Execution) {
  def execute(): View[Row] = {
    exec.execute()
  }
}

/**
 * Represent a statement. It wrap an query object and accumulate a QuerySpec
 * @tparam T
 */
class Statement[T](val spec: QuerySpec[T]) {
  def groupBy(groupbys: GroupFunc[T]*): Statement[T] = {
    assert(spec.groupbys == null)
    spec.groupbys = groupbys
    return this
  }

  def orderBy(orderbys: OrderFunc[T]*): Statement[T] = {
    assert(spec.orderbys == null)
    spec.orderbys = orderbys
    return this
  }

  def having(having: FilterFunc[T]): Statement[T] = {
    assert(spec.having == null)
    spec.having = having
    return this
  }

  def limit(limit: Limit): Statement[T] = {
    assert(spec.limit == null)
    spec.limit = limit
    return this
  }

  def compile(): Query[T] = spec.table.compile(this)
}

/**
 * Selected represents a select operation on a table. It will instantiate a Query object to represent operations on that table.
 * Subsequent operation (where, groupby, orderby, having, limit) will accumulate the states in the Query object for compilation.
 * @param table the table the select is operating on
 * @param selects the select functions
 * @tparam T type of object in the table
 */
class Selected[T](table: Table[T], selects: Seq[SelectFunc[T]]) extends Statement[T](new QuerySpec[T](table)) {
  spec.selects = selects
  selects.foreach(f => f match {
    case s: DataFactorySelectFunc[T] =>
      s.setDataFactory(table.dataFac) //Setting datafactory, and validate
    case default =>
    //do nothing
  })

  /**
   * Apply filter operation
   * @param filters
   * @return
   */
  def where(filters: FilterFunc[T]*): Statement[T] = {
    spec.filters = filters //TODO: build for immutability
    new Statement(spec)
  }
}

/**
 * A view is table-like result after a query is executed
 * @tparam T
 */
abstract class View[T] extends Table[T](null) {}

//TODO: fix data factory

/**
 * The table is abstraction of queryable source.
 * @tparam T
 */
abstract class Table[T](val dataFac: DataFactory[T]) {
  def select(selects: SelectFunc[T]*): Selected[T] = {
    new Selected[T](this, selects)
  }

  //abstracts
  def rows(): Iterator[T]

  def compile(query: Statement[T]): Query[T]
}


abstract class LazyStep[That] {
  def execute(): That
}

class IdentityStep[That](that: That) extends LazyStep[That] {
  def execute(): That = {
    that
  }
}

class Execution(val steps: ExecutionStep[_, View[Row]]) {
  def execute(): View[Row] = steps.execute()
}

class ExecutionStep[This, That](val desc: String, prev: LazyStep[This], f: (This) => That) extends LazyStep[That] {
  def execute(): That = {
    println("Executing %s".format(desc))
    val results = f.apply(prev.execute())
    println(results)
    results
  }
}

abstract class DataFactory[T] {

  case class Column[T, U: Manifest](val name: String, val select: (T) => U) {}

  def columnMap[String, Column[T, _]] = Map(getColumns().map(c => (c.name, c)): _*)

  def getColumns(): Seq[Column[T, _]]

  def getValue(value: T, s: String): Any
}

object Tabular {
  type Func[T] = (T) => Any
//  type SelectFunc[T] = (T) => Any

  type SelectFunc[T] = Func[T]

  abstract class DataFactorySelectFunc[T] extends SelectFunc[T] {
    var fac: DataFactory[T] = null

    def setDataFactory(fac: DataFactory[T]) = {
      this.fac = fac
      validate()
    }

    def validate()
  }

  type FilterFunc[T] = (T) => Boolean
  type GroupFunc[T] = Func[T]
  type OrderFunc[T] = Func[T]
  type Row = Seq[Any]
  type RowTuple = (Row, Row)
  type Limit = (Int, Int)

  case class Aggregate[T](data: T, f: (T, T) => T) {
    def aggregate(that: Aggregate[T]): Aggregate[T] = {
      new Aggregate(f.apply(data, that.data), f)
    }
  }

}
