package tabular


/**
 * Created by tiong on 5/31/14.
 */

class Query[T](table: Table[T]) {
  var selects: Seq[SelectFunc[T]] = null
  var filters: Seq[FilterFunc[T]] = null
  var groupbys: Seq[GroupFunc[T]] = null

  def compile(): View = table.compile(this)
}


class FilterFunc[T] {
}

class SelectFunc[T] {
}

class GroupFunc[T] {

}

class QueryWrapper[T](query: Query[T]) {
  def compile(): View = query.compile()
}

class Selected[T](query: Query[T], selects: Seq[SelectFunc[T]]) extends QueryWrapper[T](query) {
//  type SelectFunc[T] = (T)=>Any

  query.selects = selects

  class Where[T](query: Query[T], filters: Seq[FilterFunc[T]]) extends QueryWrapper[T](query) {
    query.filters = filters

    class GroupBy[T](query: Query[T], groupbys: Seq[GroupFunc[T]]) extends QueryWrapper[T](query) {
      query.groupbys = groupbys
    }

    def groupBy(groupbys: GroupFunc[T]*): GroupBy[T] = {
      return new GroupBy(query, groupbys)
    }
  }
//
//  def where(filters: FilterFunc[T]*) {
//    new Where(query, filters)
//  }

  def where(filters:T => Boolean) {
    null
  }
}

class View {

}

abstract class Table[T] {
  def select(selects: SelectFunc[T]*): Selected[T] = {
    new Selected[T](new Query(this), selects)
  }

  def compile(query: Query[T]): View
}

class ListTable[T](data: Seq[T]) extends Table[T] {
  override def compile(query: Query[T]): View = {
    return new View()
  }
}

class IndexedSelect[T](i: Int) extends SelectFunc[T] {
}

class NamedSelect[T](name: String) extends SelectFunc[T] {
}

class FuncSelect[T](f: T => Any) extends SelectFunc[T] {

}

class LiteralSelect[T](v: AnyVal) extends SelectFunc[T] {

}

class AllSelect[T] extends SelectFunc[T] {
}

class EqualFilter[T] extends FilterFunc[T] {

}

class FilterLHS[T](name: String) extends FilterFunc[T]{
  def eqi(that: Any): EqualFilter[T] = {
    null
  }
}

object Tabular {
  implicit def symbolSelect[T](s: Symbol): SelectFunc[T] = s.name match {
    case "*" => new AllSelect[T]()
    case _ => new NamedSelect[T](s.name)
  }

  implicit def funcSelect[T](f: T => Any): SelectFunc[T] = new FuncSelect[T](f)

  implicit def anyValSelect[T](v: AnyVal): SelectFunc[T] = new LiteralSelect[T](v)

  implicit def symbolToFilterLHS[T](s: Symbol): FilterLHS[T] = new FilterLHS[T](s.name)
}
