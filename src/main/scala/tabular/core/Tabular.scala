package tabular.core

import tabular.core.QuerySupport.{FuncSelect, AliasSelect, NamedSelect}
import tabular.core.Tabular._
import tabular.execution._
import tabular.util.{RowSorter, Utils}

import scala.collection.immutable.ListMap

/**
 * Tabular is an abstraction of table-like structure, which contains rows of data of type T.
 * The data factory facilitate the data extraction from T. For example, if T is a Person type,
 * data factory provides definition to extract attributes (firstName, lastName etc) from the Person type.
 *
 * The table-like structure provide a facility to compile a sql-like Statement into an executable Query
 * @param dataFac the schema
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

  def join[U](tab: Tabular[U]): JoinedTabular[T, U]

  /**
   * Compile a Statement into an executable Query.
   * @param stmt the statement
   * @return query that can be executed and get result of the statement
   */
  def compile[U](stmt: Statement[T], rowFac: RowFactory[U] = new DefaultRowFactory): View[U]

}


abstract class JoinedTabular[A, B](tab1: Tabular[A], tab2: Tabular[B]) extends Tabular[(A, B)](new JoinedDataFactory[A, B](tab1, tab2)){
  def on(funcA: SelectFunc[A], funcB: SelectFunc[B]): JoinedTabular[A, B]
}

class JoinedDataFactory[T, U](tab1: Tabular[T], tab2: Tabular[U]) extends DataFactory[(T, U)] {
  val columns = (tab1.dataFac.getColumns().map {
    column =>
      new Column("table1." + column.name, new FuncSelect[(T, U)]( tuple => column.select.apply(tuple._1) ))
  } ++ tab2.dataFac.getColumns().map {
    column =>
      new Column("table2." + column.name, new FuncSelect[(T, U)]( tuple => column.select.apply(tuple._2) ))
  }).asInstanceOf[Seq[Column[(T, U), SelectFunc[(T, U)]]]]

  override def getColumns(): Seq[Column[(T, U), SelectFunc[(T, U)] ]] = columns

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

  def getValue(value: T, s: Symbol): Any = {
    columnMap(s) match {
      case func: SelectFunc[T] =>
        func.apply(value)
      case _ =>
        throw new IllegalStateException("Cannot find value")
    }
  }
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

  def compile(): View[Row] = spec.table.compile(this)
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
abstract class View[T](fac: DataFactory[T], val plan: ExecutionPlan[Table[T]]) extends Tabular[T](fac) {
  def materialize(): Table[T]
}

//TODO: fix data factory

/**
 * The table is abstraction of queryable source.
 * @tparam T
 */
abstract class Table[T](val fac: DataFactory[T]) extends Tabular[T](fac)

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
    val columns = nameToColumns.values.toSeq.asInstanceOf[Seq[Column[Row, SelectFunc[Row]]]]

    /** get the columns **/
    override def getColumns(): Seq[Column[Row, SelectFunc[Row]]] = columns


    override def getValue(value: Row, s: Symbol): Any = nameToColumns(s).select.apply(value)
  }

  case class Aggregate[T](data: T, f: (T, T) => T) {
    def aggregate(that: Aggregate[T]): Aggregate[T] = {
      new Aggregate(f.apply(data, that.data), f)
    }
  }

  abstract class RowFactory[U] {
    def createDataFactory[T](spec: QuerySpec[T]): DataFactory[U]

    def sort[T](spec: QuerySpec[T], data: Seq[U]): Seq[U]

    def groupBy[T](spec: QuerySpec[T], data: Seq[U]): Seq[U]

    def createRow[T](spec: QuerySpec[T], value: T): U
  }

  class DefaultRowFactory extends RowFactory[Row] {

    type RowkeyFunc = (Row) => Row

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

    class IndicesRowkeyFunc(indicies: Seq[Int]) extends RowkeyFunc {
      override def apply(v1: Row): Row = {
        indicies.map(v1(_))
      }
    }

    override def createRow[T](spec: QuerySpec[T], value: T): Row  = spec.selects.map(_.apply(value))

    override def groupBy[T](spec: QuerySpec[T], data: Seq[Row]): Seq[Row] = {
      val groupIndices: Seq[Int] = Utils.getIndices(spec.getSelectFieldNames, spec.groupbys)
      val groupData = data.groupBy(new IndicesRowkeyFunc(groupIndices))
      val aggregatedData = groupData.map(t => aggregate(t._2))
      aggregatedData.toSeq
    }

    private def aggregate(data: Seq[Row]): Row = {
      val results = data.reduce((a: Row, b: Row) => {
        a.zip(b).map(t => if (classOf[Aggregate[Int]].isInstance(t._1)) {
          t._1.asInstanceOf[Aggregate[Int]].aggregate(t._2.asInstanceOf[Aggregate[Int]])
        } else t._1)
      })
      results.map(a => if (classOf[Aggregate[Int]].isInstance(a)) a.asInstanceOf[Aggregate[Int]].data else a)
    }

    override def sort[T](spec: QuerySpec[T], data: Seq[Row]): Seq[Row] = {
      val sortIndices = Utils.getIndices(spec.getSelectFieldNames, spec.orderbys)
      data.sortWith {
        new RowSorter(sortIndices)
      }
    }

    override def createDataFactory[T](spec: QuerySpec[T]): DataFactory[Row] = {
      new RowDataFactory(spec.getSelectFieldNames.map(_.name))
    }
  }
}
