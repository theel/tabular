package tabular.core

import tabular.core.QuerySupport.{AliasSelect, NamedSelect}
import tabular.core.Tabular._

import scala.collection.immutable.ListMap

/**
 * Tabular is an abstraction of table-like structure, which contains rows of data of type T.
 * The data factory facilitate the data extraction from T. For example, if T is a Person type,
 * data factory provides definition to extract attributes (firstName, lastName etc) from the Person type.
 *
 * The table-like structure provide a facility to compile a sql-like Statement into an executable Query
 * @param dataFac the data factory
 * @tparam T type of rows
 */
abstract class Tabular[T](val dataFac: DataFactory[T]) {


  def select(selects: SelectFunc[T]*): Selected[T] = {
    new Selected[T](this, selects)
  }

  class FuncSelect[T](f: (T) => Any) extends SelectFunc[T] {
    override def apply(v1: T): Any = f.apply(v1)
  }

  /**
   * @return rows of data of type T
   */
  def rows(): Iterator[T]

  /**
   * Compile a Statement into an executable Query.
   * @param stmt the statement
   * @return query that can be executed and get result of the statement
   */
  def compile(stmt: Statement[T]): Query[T]

}


/**
 * A DataFactory providers metadata and allow extraction of value by their field names
 * @tparam T
 */

abstract class DataFactory[T] {

  case class Column[T, U: Manifest](val name: String, val select: (T) => U) {}

  final def columnMap[Symbol, Column[T, _]] = ListMap(getColumns().map(c => (Symbol(c.name), c)): _*) //we need the ordering

  def getColumns(): Seq[Column[T, _]]

  def getColumnNames(): Seq[String] = columnMap.keys.map(_.name).toSeq

  def getValue(value: T, s: Symbol): Any
}

/**
 * Represent a statement that is declared against a table. It wrap an QuerySpec object and accumulate query states into the QuerySpec object
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

  def having(having: FilterFunc[Row]*): Statement[T] = {
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
 * Represent query state. It contains
 * 1. The target table
 * 2. select
 * 3. filters
 * 4. group by
 * 5. having
 * 6. order by
 * 7. limit
 * @tparam T the type T
 */
case class QuerySpec[T](val table: Tabular[T]) extends Cloneable {
  var limit: Limit = null
  var having: Seq[FilterFunc[Row]] = null
  var orderbys: Seq[Symbol] = null
  var selects: Seq[SelectFunc[T]] = null
  var filters: Seq[FilterFunc[T]] = null
  var groupbys: Seq[Symbol] = null

  def getSelectFieldNames: List[Symbol] = selects.zipWithIndex.map {
    case (value, index) => value match {
      case sym: NamedSelect[T] =>
        sym.name
      case alias: AliasSelect[T] =>
        alias.name
      case default =>
        Symbol("field%d".format(index))
    }
  }.toList

}

/**
 * Represent a query that is compiled. The query contains execution plan that can executed to get
 * subsequent View[Row]
 * @tparam T
 */
class Query[T](val spec: QuerySpec[T], val exec: Execution) {
  def execute(): View[Row] = {
    exec.execute()
  }
}


/**
 * Selected is always the first Statement created out of a table/view.
 * Subsequent operation (where, groupby, orderby, having, limit) will accumulate the states in the QuerySpec object for compilation.
 * @param table the table the select is operating on
 * @param selects the select functions
 * @tparam T type of object in the table
 */
class Selected[T](table: Tabular[T], selects: Seq[SelectFunc[T]]) extends Statement[T](new QuerySpec[T](table)) {
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

  /**
   * Apply filter operation
   * @param filters
   * @return
   */
  def where_(filters: FilterFunc[T]*): Statement[T] = {
    spec.filters = filters.asInstanceOf[Seq[FilterFunc[T]]] //TODO: build for immutability
    new Statement(spec)
  }

}



/**
 * A view is table-like result after a query is executed
 * @tparam T
 */
abstract class View[T](val df: DataFactory[T]) extends Tabular[T](df)

//TODO: fix data factory

/**
 * The table is abstraction of queryable source.
 * @tparam T
 */
abstract class Table[T](val fac: DataFactory[T]) extends Tabular[T](fac)


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

object Tabular {

  /**
   * Represent a select function
   * @tparam T
   */
  type SelectFunc[T] = (T) => Any

  /**
   * Represent a filter function
   * @tparam T
   */
  type FilterFunc[T] = (T) => Boolean

  /**
   * Represent limit
   */
  type Limit = (Int, Int)


  /**
   * A select function that directly interact with DataFactory
   * @tparam T
   */
  abstract class DataFactorySelectFunc[T] extends SelectFunc[T] {
    var fac: DataFactory[T] = null

    def setDataFactory(fac: DataFactory[T]) = {
      this.fac = fac
      validate()
    }

    def validate()
  }

  /**
   * Represent a row
   */
  type Row = Seq[Any]

  /**
   * Represent row tuples
   */
  type RowTuple = (Row, Row)

  /**
   * Represent a grouped tuples
   */
  type GroupedRows = (Row, Seq[Row])


  /**
   * A generic data factory operating on Row object
   */
  class RowDataFactory(fieldNames: Seq[String]) extends DataFactory[Row] {

    /** name-column mappings **/
    val nameToColumns = fieldNames.zipWithIndex.map { case (name, index) => (Symbol(name), new Column[Row, Any](name, row => row(index)))}.toMap

    /** the columns **/
    val columns = nameToColumns.values.toSeq

    /** get the columns **/
    override def getColumns(): Seq[Column[Row, _]] = columns

    override def getValue(value: Row, s: Symbol): Any = nameToColumns(s).select.apply(value)
  }

  case class Aggregate[T](data: T, f: (T, T) => T) {
    def aggregate(that: Aggregate[T]): Aggregate[T] = {
      new Aggregate(f.apply(data, that.data), f)
    }
  }


}
