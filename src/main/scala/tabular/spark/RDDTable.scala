package tabular.spark

import org.apache.spark.rdd.RDD
import tabular.core.QuerySupport.AndFilterFunc
import tabular.core.Tabular._
import tabular.core._
import tabular.execution.ExecutionPlan
import tabular.util.Utils.{IndicesRowkeyFunc, RowAggregator}
import tabular.util.{RowOrdering, RowSorter, Utils}
import scala.reflect.ClassTag
import scala.reflect.classTag

/**
  * A list implementation of tabular framework. The table wraps a list of T and allow executing sql query to it
  * @param data a list of T
  * @param fac the data factory for T
  * @tparam T the type
  */
class RddTable[T: ClassTag](val data: RDD[T], fac: DataFactory[T]) extends Table[T](fac) {

   //TODO: Support row factory
   override def compile(stmt: Statement[T]): View[Row] = {
     val spec = stmt.spec
     import tabular.execution.ExecutionPlanning._

     val selectFieldNames = spec.getSelectFieldNames
     val fac = new RowDataFactory(spec.getSelectFieldNames.map(_.name))
     val plan = Plan[Table[Row]]("Plan for " + stmt.toString) {
       Step[RDD[T]]("Identity step") {
         data
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

       Step[RDD[Row], RddTable[Row]]("Finalize") {
         rdd =>
          new RddTable[Row](rdd.asInstanceOf[RDD[Row]], fac)
       }
     }
     //TODO: WIP regarding execution step
     new RddView[Row](fac, plan)
   }

   override def rows(): Iterator[T] = data.collect().iterator

   override def join[U: ClassTag](tab: Tabular[U]): JoinedTabular[T, U] =
   {
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

  private def selectOp[T](selects: Seq[SelectFunc[T]], value: T): Row  = selects.map(_.apply(value))

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


import scala.reflect._
class RddJoinedTabular[A: ClassTag, B: ClassTag](t1: RddTable[A], t2: RddTable[B]) extends JoinedTabular[A, B](t1, t2){
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
        t1.data.cartesian(t2.data)
       case _ =>
         val t1rows = t1.data.map(row => (funcA.apply(row), row))
         val t2rows = t2.data.map(row => (funcB.apply(row), row))
         t1rows.join(t2rows).map(_._2)
     }
   }

   override def join[U: ClassTag](tab: Tabular[U]): JoinedTabular[(A, B), U] = ??? //new RddJoinedTabular[(A, B), U](this, tab)

  import scala.reflect.classTag
   /**
    * Compile a Statement into an executable Query.
    * @param stmt the statement
    * @return query that can be executed and get result of the statement
    */
   override def compile(stmt: Statement[(A, B)]): View[Row] = {
     //TODO: Need to optimize this
     new RddTable(joinRdd(), dataFac).compile(stmt)
   }

   override def select(selects: SelectFunc[(A, B)]*): Selected[(A, B)] = super.select(selects: _*)

   override def on(fa: SelectFunc[A], fb: SelectFunc[B]): RddJoinedTabular[A, B] = {
     if (funcA!=null){
       funcA = fa
       funcB = fb
     } else {
       throw new IllegalArgumentException("On specification is set")
     }
     this
   }
 }

class RddView[T](fac: DataFactory[T], plan: ExecutionPlan[Table[T]]) extends View[T](fac, plan) {
   lazy val impl = plan.execute()

   //abstracts
   override def rows(): Iterator[T] = impl.rows()

   //TODO: work on chaining plan
   override def compile(stmt: Statement[T]): View[Row] = impl.compile(stmt)

   override def toString(): String = {
     val columns = fac.getColumns().map(c => "\"%s\"".format(c.name)).mkString(",")
     "RddView[%s]\n%s".format(columns, impl.rows().toSeq.mkString("\n"))
   }

   override def materialize(): Table[T] = impl

   override def join[U: ClassTag](tab: Tabular[U]): JoinedTabular[T, U] = impl.join(tab)
 }