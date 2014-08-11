package tabular

import tabular.Tabular._

import scala.collection.immutable.ListMap

/**
 * Represent query state
 * @tparam T
 */
case class QuerySpec[T](val table: Table[T]) extends Cloneable {
  var limit: Limit = null
  var having: Seq[AFilterFunc[T]] = null
  var orderbys: Seq[Symbol] = null
  var selects: Seq[ASelectFunc[T]] = null
  var filters: Seq[AFilterFunc[T]] = null
  var groupbys: Seq[Symbol] = null
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
  def groupBy(groupbys: Symbol*): Statement[T] = {
    assert(spec.groupbys == null)
    spec.groupbys = groupbys
    return this
  }

  def orderBy(orderbys: Symbol*): Statement[T] = {
    assert(spec.orderbys == null)
    spec.orderbys = orderbys
    return this
  }

  def having(having: AFilterFunc[T]*): Statement[T] = {
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
class Selected[T](table: Table[T], selects: Seq[ASelectFunc[T]]) extends Statement[T](new QuerySpec[T](table)) {
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
  def where(filters: AFilterFunc[T]*): Statement[T] = {
    spec.filters = filters //TODO: build for immutability
    new Statement(spec)
  }

  /**
   * Apply filter operation
   * @param filters
   * @return
   */
  def where_(filters: FilterFunc[T]*): Statement[T] = {
    spec.filters = filters.asInstanceOf[Seq[AFilterFunc[T]]] //TODO: build for immutability
    new Statement(spec)
  }

}

abstract class Tabular[T](val dataFac: DataFactory[T]) {


  def rows(): Iterator[T]

  def compile(stmt: Statement[T]): Query[T]

}

/**
 * A view is table-like result after a query is executed
 * @tparam T
 */
abstract class View[T](val df: DataFactory[T]) extends Tabular[T](df) {}

//TODO: fix data factory

/**
 * The table is abstraction of queryable source.
 * @tparam T
 */
abstract class Table[T](val fac: DataFactory[T]) extends Tabular[T](fac) {

  def select(selects: ASelectFunc[T]*): Selected[T] = {
    new Selected[T](this, selects)
  }

  def select_(selects: SelectFunc[T]*): Selected[T] = {
    new Selected[T](this, mapOrWrap(selects))
  }

  class FuncSelect[T](f: (T) => Any) extends ASelectFunc[T] with SelectFunc[T] {
    override def apply(v1: T): Any = f.apply(v1)
  }

  def mapOrWrap(selects: Seq[SelectFunc[T]]): Seq[ASelectFunc[T]] = {
    selects.map { func =>
      func match {
        case a: ASelectFunc[T] => a
        case default => new FuncSelect[T](func)
      }
    }
  }

}


abstract class LazyStep[That] {
  def execute(): Tabular[That]
}

class IdentityStep[That](that: Tabular[That]) extends LazyStep[That] {
  def execute(): Tabular[That] = {
    that
  }
}

class Execution(val steps: ExecutionStep[_, Row]) {
  def execute(): View[Row] = steps.execute()
}

class ExecutionStep[This, That](val desc: String, prev: LazyStep[This], f: Tabular[This] => View[That]) extends LazyStep[That] {
  def execute(): View[That] = {
    val results = f.apply(prev.execute())
    results
  }
}

abstract class DataFactory[T] {

  case class Column[T, U: Manifest](val name: String, val select: (T) => U) {}

  final def columnMap[String, Column[T, _]] = ListMap(getColumns().map(c => (c.name, c)): _*) //we need the ordering

  def getColumns(): Seq[Column[T, _]]

  def getColumnNames(): Seq[String] = columnMap.keys.toSeq

  def getValue(value: T, s: String): Any
}

object Tabular {
  type Func[T] = (T) => Any
  type SelectFunc[T] = (T) => Any


  abstract class DataFactorySelectFunc[T] extends ASelectFunc[T] with SelectFunc[T] {
    var fac: DataFactory[T] = null

    def setDataFactory(fac: DataFactory[T]) = {
      this.fac = fac
      validate()
    }

    def validate()
  }

  type FilterFunc[T] = (T) => Boolean

  trait ASelectFunc[T]

  trait AFilterFunc[T]

  //  abstract class GroupFunc[T] extends Func[T]

  //  abstract class OrderFunc[T] extends Func[T]

  type Row = Seq[Any]
  type RowTuple = (Row, Row)
  type GroupedRows = (Row, Seq[Row])
  type Limit = (Int, Int)

  case class Aggregate[T](data: T, f: (T, T) => T) {
    def aggregate(that: Aggregate[T]): Aggregate[T] = {
      new Aggregate(f.apply(data, that.data), f)
    }
  }

}
