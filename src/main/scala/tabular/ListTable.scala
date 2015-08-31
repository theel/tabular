package tabular

import tabular.core.QuerySupport.AndFilterFunc
import tabular.core.Tabular._
import tabular.core._
import tabular.execution.ExecutionPlan

/**
 * A list implementation of tabular framework. The table wraps a list of T and allow executing sql query to it
 * @param data a list of T
 * @param fac the data factory for T
 * @tparam T the type
 */
class ListTable[T](data: Seq[T], fac: DataFactory[T]) extends Table[T](fac) {

  //TODO: Support row factory
  override def compile[U](stmt: Statement[T], rowFactory: RowFactory[U] = new DefaultRowFactory): View[U] = {
    val spec = stmt.spec
    import tabular.execution.ExecutionPlanning._

    val selectFieldNames = spec.getSelectFieldNames
    val fac = rowFactory.createDataFactory(spec)
    val plan = Plan[Table[U]]("Plan for " + stmt.toString) {
      Step[Seq[T]]("Identity step") {
        data
      }
      //step 1 - filter
      if (spec.filters != null) {
        Step[Seq[T], Seq[T]]("Filtering") {
          _.filter(new AndFilterFunc[T](spec.filters.asInstanceOf[Seq[FilterFunc[T]]]))
        }
      }

      //step 2 - project
      Step[Seq[T], Seq[U]]("Selecting fields") {
        _.map(t => rowFactory.createRow(spec, t))
      }

      //step 3 - groupby
      if (spec.groupbys != null) {
        //TODO: Fix type U
        Step[Seq[U], Seq[U]]("Group by") {
          rowFactory.groupBy(spec, _)
          //TODO: implement having
        }
      }
      //TODO: ordering, and limit
      if (spec.orderbys != null) {
        Step[Seq[U], Seq[U]]("Order by") {
          rowFactory.sort(spec, _)
        }
      }

      Step[Seq[U], Table[U]]("Finalize") {
        new ListTable[U](_, fac)
      }
    }
    //TODO: WIP regarding execution step
    new ListView[U](fac, plan)
  }

  override def rows(): Iterator[T] = data.iterator

  override def join[U](tab: Tabular[U]): JoinedTabular[T, U] = ???
//  {
//    new ListJoinedTabular[T, U](this, tab);
//  }
}

class ListView[T](fac: DataFactory[T], plan: ExecutionPlan[Table[T]]) extends View[T](fac, plan) {
  lazy val impl = plan.execute()

  //abstracts
  override def rows(): Iterator[T] = impl.rows()

  //TODO: work on chaining plan
  override def compile[U](stmt: Statement[T], rowFac: RowFactory[U]): View[U] = impl.compile(stmt, rowFac)

  override def toString(): String = {
    val columns = fac.getColumns().map(c => "\"%s\"".format(c.name)).mkString(",")
    "ListView[%s]\n%s".format(columns, impl.rows().toSeq.mkString("\n"))
  }

  override def materialize(): Table[T] = impl

  override def join[U](tab: Tabular[U]): JoinedTabular[T, U] = impl.join(tab)
}

abstract class ListJoinedTabular[T, U](tab1: Tabular[T], tab2: Tabular[U]) extends JoinedTabular[T, U](tab1, tab2) {
  /**
   * @return rows of data of type T
   */
  override def rows(): Iterator[(T, U)] = ???

  override def join[V](tab: Tabular[V]): JoinedTabular[(T, U), V] = ???

}