package tabular.spark

import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import tabular.core.QuerySupport.AndFilterFunc
import tabular.core.Table._
import tabular.core._
import tabular.execution.ExecutionPlanning._
import tabular.execution.{ExecutionPlan, IdentityStep}
import tabular.util.Utils.{IndicesRowkeyFunc, RowAggregator}
import tabular.util.{RowOrdering, RowSorter, Utils}

import scala.reflect.{ClassTag, classTag}

/**
 * A list implementation of tabular framework. The table wraps a list of T and allow executing sql query to it
 * @param fac the data factory for T
 * @tparam T the type
 */
class RddTable[T: ClassTag](val plan: ExecutionPlan[RDD[T]], fac: DataFactory[T]) extends Table[T](fac) {

  def this(data: RDD[T], fac: DataFactory[T]) = this(
    new ExecutionPlan[RDD[T]]("source", new IdentityStep[RDD[T]](data)), fac)

  //TODO: Support row factory
  override def compile(stmt: Statement[T]): Table[Row] = {
    val spec = stmt.spec

    val selectFieldNames = spec.getSelectFieldNames
    val fac = new RowDataFactory(spec.getSelectFieldNames.map(_.name))
    val plan = Plan[RDD[Row]]("Plan for " + stmt.toString) {
      Step[RDD[T]]("Identity step") {
        this.plan.execute()
      }

      //step 1 - filter
      if (spec.filters != null) {
        Step[RDD[T], RDD[T]]("Filtering") {
          _.filter(new AndFilterFunc[T](spec.filters.asInstanceOf[Seq[FilterFunc[T]]]))
        }
      }

      //step 2 - project
      Step[RDD[T], RDD[Row]]("Selecting fields") {
        _.map(t => spec.selects.map(_.apply(t)))
      }

      //step 3 - groupby
      if (spec.groupbys != null) {
        //TODO: Fix type U
        Step[RDD[Row], RDD[Row]]("Group by") {
          groupByOp(spec, _)
          //TODO: implement having
        }
      }
      //TODO: ordering, and limit
      if (spec.orderbys != null) {
        Step[RDD[Row], RDD[Row]]("Order by") {
          val sortIndices = Utils.getIndices(spec.getSelectFieldNames, spec.orderbys)
          _.sortBy {
            row =>
              sortIndices.map(row(_))
          }(RowOrdering, classTag[Row])
        }
      }
    }
    //TODO: WIP regarding execution step
    new RddTable[Row](plan, fac)
  }

  override def rows(): Iterator[T] = plan.execute().collect.iterator

  override def join[U: ClassTag](tab: Table[U]): JoinedTable[T, U] = {
    join(tab.asInstanceOf[RddTable[U]])
  }


  private def aggregate(data: Seq[Row]): Row = {
    val results = data.reduce((a: Row, b: Row) => {
      a.zip(b).map(t => if (classOf[Aggregate[Int]].isInstance(t._1)) {
        t._1.asInstanceOf[Aggregate[Int]].aggregate(t._2.asInstanceOf[Aggregate[Int]])
      } else t._1)
    })
    results.map(a => if (classOf[Aggregate[Int]].isInstance(a)) a.asInstanceOf[Aggregate[Int]].data else a)
  }

  private def selectOp[T](selects: Seq[SelectFunc[T]], value: T): Row = selects.map(_.apply(value))

  type RowkeyFunc = (Row) => Row


  private def sort[T](spec: QuerySpec[T], data: Seq[Row]): Seq[Row] = {
    val sortIndices = Utils.getIndices(spec.getSelectFieldNames, spec.orderbys)
    data.sortWith {
      new RowSorter(sortIndices)
    }
  }

  private def groupByOp[T](spec: QuerySpec[T], data: RDD[Row]): RDD[Row] = {
    val groupIndices: Seq[Int] = Utils.getIndices(spec.getSelectFieldNames, spec.groupbys)
    val keyFunc = new IndicesRowkeyFunc(groupIndices)
    val groupData = data.map(row => (keyFunc.apply(row), row))
    groupData.reduceByKey(new RowAggregator).map(t => Utils.normalizeAggregate(t._2))
  }
}


class RddJoinedTable[A, B: ClassTag](t1: RddTable[A], t2: RddTable[B]) extends JoinedTable[A, B](t1, t2) {
  var funcA: SelectFunc[A] = null

  var funcB: SelectFunc[B] = null

  /**
   * @return rows of data of type T
   */
  override def rows(): Iterator[(A, B)] = {
    joinRdd().collect().iterator
  }

  def joinRdd(): RDD[(A, B)] = {
    funcA match {
      case null =>
        val t1data = t1.plan.execute()
        val t2data = t2.plan.execute()
        t1data.cartesian(t2data)
      case _ =>
        val t1data = t1.plan.execute()
        val t2data = t2.plan.execute()
        val t1rows = t1data.map(row => (funcA.apply(row), row)).asInstanceOf[PairRDDFunctions[Any, A]] //explicitly cast since scala seems to have problem with implicit here
        val t2rows = t2data.map(row => (funcB.apply(row), row))
        t1rows.join[B](t2rows).map(_._2)
    }
  }

  override def join[U: ClassTag](tab: Table[U]): JoinedTable[(A, B), U] = ??? //new RddJoinedTabular[(A, B), U](this, tab)

  /**
   * Compile a Statement into an exect the statement
   * @return query that can be executed and get result of the statement
   */
  override def compile(stmt: Statement[(A, B)]): Table[Row] = {
    //TODO: Need to optimize this
    new RddTable(joinRdd(), dataFac).compile(stmt)
  }

  override def select(selects: SelectFunc[(A, B)]*): Selected[(A, B)] = super.select(selects: _*)

  override def on(fa: SelectFunc[A], fb: SelectFunc[B]): RddJoinedTable[A, B] = {
    if (funcA != null) {
      funcA = fa
      funcB = fb
    } else {
      throw new IllegalArgumentException("On specification is set")
    }
    this
  }
}