package tabular

import tabular.Tabular._

/**
 * Represent query state
 * @tparam T
 */
case class QuerySpec[T](val table: Table[T]) extends Cloneable{
  var limit: Limit = null
  var having: FilterFunc[T] = null
  var orderbys: Seq[OrderFunc[T]] = null
  var selects: Seq[SelectFunc[T]] = null
  var filter: FilterFunc[T] = null
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
class Statement[T](val spec: QuerySpec[T]){
  def groupBy(groupbys: GroupFunc[T]*): Statement[T] = {
    assert(spec.groupbys==null)
    spec.groupbys = groupbys
    return this
  }

  def orderBy(orderbys: OrderFunc[T]*): Statement[T] = {
    assert(spec.orderbys==null)
    spec.orderbys = orderbys
    return this
  }

  def having(having: FilterFunc[T]): Statement[T] = {
    assert(spec.having==null)
    spec.having = having
    return this
  }

  def limit(limit: Limit): Statement[T] = {
    assert(spec.limit==null)
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
  selects.foreach(f => println("Select = " + f.getClass))

  /**
   * Apply filter operation
   * @param filter
   * @return
   */
  def where(filter: FilterFunc[T]): Statement[T] = {
    spec.filter = filter //TODO: build for immutability
    new Statement(spec)
  }
}

/**
 * A view is table-like result after a query is executed
 * @tparam T
 */
abstract class View[T] extends Table[T](null) {} //TODO: fix data factory

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
    f.apply(prev.execute())
  }
}

abstract class DataFactory[T] {
  case class Column[T, U: Manifest](val name: String, val f: (T) => U){}
  def getColumns(): Seq[Column[T, _]]
  def getValue(value: T, s: String): Any
}

object Tabular {
  type Func[T] = (T) => Any
//  abstract class SelectFunc[T] extends Func[T] {
//    var fac: DataFactory[T] = null
//  }
  type SelectFunc[T] = (T) => Any
  type FilterFunc[T] = (T) => Boolean
  type GroupFunc[T] = (T) => Any
  type OrderFunc[T] = (T) => Any
  type Row = Seq[Any]
  type RowTuple = (Row, Row)
  type Limit = (Int, Int)

  case class Aggregate[T](data: T, f: (T, T) => T) {
    def aggregate(that: Aggregate[T]): Aggregate[T] = {
      new Aggregate(f.apply(data, that.data), f)
    }
  }

}
