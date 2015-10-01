package tabular.list

import tabular.core.QuerySupport.AndFilterFunc
import tabular.core.Table._
import tabular.core._
import tabular.execution.{ExecutionPlan, IdentityStep}
import tabular.util.Utils.IndicesRowkeyFunc
import tabular.util.{RowSorter, Utils}

import scala.reflect.ClassTag

/**
 * A list implementation of tabular framework. The table wraps a list of T and allow executing sql query to it
 * @param plan the execution plan for the table
 * @param fac the data factory for T
 * @tparam T the type
 */
class ListTable[T](plan: ExecutionPlan[Seq[T]], fac: DataFactory[T]) extends Table[T](fac) {

  def this(data: Seq[T], fac: DataFactory[T]) = this(
    new ExecutionPlan[Seq[T]]("source", new IdentityStep[Seq[T]](data)), fac)

  //TODO: Support row factory
  override def compile(stmt: Statement[T]): Table[Row] = {
    val spec = stmt.spec
    import tabular.execution.ExecutionPlanning._

    val selectFieldNames = spec.getSelectFieldNames
    val fac = new RowDataFactory(spec.getSelectFieldNames.map(_.name))
    val plan = Plan[Seq[Row]]("Plan for " + stmt.toString) {
      Step[Seq[T]]("Identity step") {
        this.plan.execute()
      }
      //step 1 - filter
      if (spec.filters != null) {
        Step[Seq[T], Seq[T]]("Filtering") {
          _.filter(new AndFilterFunc[T](spec.filters.asInstanceOf[Seq[FilterFunc[T]]]))
        }
      }

      //step 2 - project
      Step[Seq[T], Seq[Row]]("Selecting fields") {
        _.map(t => selectOp(spec.selects, t))
      }

      //step 3 - groupby
      if (spec.groupbys != null) {
        //TODO: Fix type U
        Step[Seq[Row], Seq[Row]]("Group by") {
          groupByOp(spec, _)
          //TODO: implement having
        }
      }
      //TODO: ordering, and limit
      if (spec.orderbys != null) {
        Step[Seq[Row], Seq[Row]]("Order by") {
          sort(spec, _)
        }
      }
    }
    //TODO: WIP regarding execution step
    new ListTable[Row](plan, fac)
  }

  private def aggregate(data: Seq[Row]): Row = {
    val results = data.reduce((a: Row, b: Row) => {
      a.zip(b).map(t => if (classOf[Aggregate[Int]].isInstance(t._1)) {
        t._1.asInstanceOf[Aggregate[Int]].aggregate(t._2.asInstanceOf[Aggregate[Int]])
      } else t._1)
    })
    results.map(a => if (classOf[Aggregate[Int]].isInstance(a)) a.asInstanceOf[Aggregate[Int]].data else a)
  }

  private def selectOp[T](selects: Seq[SelectFunc[T]], value: T): Row  = selects.map(_.apply(value))


  private def sort[T](spec: QuerySpec[T], data: Seq[Row]): Seq[Row] = {
    val sortIndices = Utils.getIndices(spec.getSelectFieldNames, spec.orderbys)
    data.sortWith {
      new RowSorter(sortIndices)
    }
  }

  private def groupByOp[T](spec: QuerySpec[T], data: Seq[Row]): Seq[Row] = {
    val groupIndices: Seq[Int] = Utils.getIndices(spec.getSelectFieldNames, spec.groupbys)
    val groupData = data.groupBy(new IndicesRowkeyFunc(groupIndices))
    val aggregatedData = groupData.map(t => aggregate(t._2))
    aggregatedData.toSeq
  }

  override def rows(): Iterator[T] = plan.execute().iterator

  override def join[U: ClassTag](tab: Table[U]): JoinedTable[T, U] =
  {
    new ListJoinedTabular[T, U](this, tab)
  }
}

class ListJoinedTabular[A, B](t1: Table[A], t2: Table[B])  extends JoinedTable[A, B](t1, t2){
  var funcA: SelectFunc[A] = (a) => 1

  var funcB: SelectFunc[B] = (b) => 1
  /**
   * @return rows of data of type T
   */
  override def rows(): Iterator[(A, B)] = {
    val t2rows = t2.rows().toSeq.map(row => (funcB.apply(row), row))
    t1.rows().flatMap {
      a =>
        val aKey = funcA.apply(a)
        t2rows.flatMap{
          case (bKey, b) =>
            if (aKey == bKey) Seq((a, b)) else Seq()
        }
    }.toIterator
  }

  override def join[U: ClassTag](tab: Table[U]): JoinedTable[(A, B), U] = new ListJoinedTabular[(A, B), U](this, tab)

  /**
   * Compile a Statement into an executable Query.
   * @param stmt the statement
   * @return query that can be executed and get result of the statement
   */
  override def compile(stmt: Statement[(A, B)]): Table[Row] = {
    //TODO: Need to optimize this
    new ListTable(rows.toList, dataFac).compile(stmt)
  }

  override def select(selects: SelectFunc[(A, B)]*): Selected[(A, B)] = super.select(selects: _*)

  override def on(fa: SelectFunc[A], fb: SelectFunc[B]): ListJoinedTabular[A, B] = {
    if (funcA!=null){
      funcA = fa
      funcB = fb
    } else {
      throw new IllegalArgumentException("On specification is set")
    }
    this
  }
}