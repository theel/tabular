package tabular

import tabular.Tabular._

class ListTable[T](val data: Seq[T], val fac: DataFactory[T] = null) extends Table[T](fac) {

  override def compile(stmt: Statement[T]): Query[T] = {
    val spec = stmt.spec
    val identity = new IdentityStep[Seq[T]](data)
    val filteredStep =
      if (spec.filter != null) {
        new ExecutionStep[Seq[T], Seq[T]]("Filter with %s".format(spec.filter), identity, _.filter(spec.filter))
      } else {
        identity
      }
    if (spec.groupbys != null) {
      //project group->select tuples
      val projectGroupStep = new ExecutionStep[Seq[T], Seq[RowTuple]]("Project groups (%s) -> (%s)".format(spec.groupbys.mkString(","), spec.selects.mkString(",")),
        filteredStep, _.map(row => (spec.groupbys.map(func => func.apply(row)) ->
          spec.selects.map(func => func.apply(row)))))
      val groupByStep = new ExecutionStep[Seq[RowTuple], Map[Row, Seq[RowTuple]]]("Group by group keys",
        projectGroupStep, _.groupBy(_._1))
      val finalStep = new ExecutionStep[Map[Row, Seq[RowTuple]], View[Row]]("Aggregated rows", groupByStep,
        groups =>
          new ListView(groups.map(a => aggregate(a._2.map(_._2))).toSeq))
      new Query(spec, new Execution(finalStep))
    } else {
      val finalStep = new ExecutionStep[Seq[T], View[Row]]("Select " + spec.selects, filteredStep,
        groups => new ListView(groups.map(row => spec.selects.map(func => func.apply(row)))))
      return new Query(spec, new Execution(finalStep))
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

  override def compile(stmt: Statement[Row]): Query[Row] = impl.compile(stmt)
}