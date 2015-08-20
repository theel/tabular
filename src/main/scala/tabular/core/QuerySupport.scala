package tabular.core

import tabular.core.Tabular._

/**
 * Created by tiong on 7/24/14.
 */
object QuerySupport {

  //** Select support **
  // Func conversion
  implicit def funcSelect[T](p: (T) => Any): SelectFunc[T] = new FuncSelect[T](p)

  // Literal conversion
  implicit def stringSelect[T](v: String): SelectFunc[T] = new StringLiteralSelect[T](v)
  implicit def anyValSelect[T](v: AnyVal): SelectFunc[T] = new AnyValSelect[T](v)

  // Symbol conversion
  implicit def symbolSelect[T](s: Symbol): SelectFunc[T] = s.name match {
    case "*" => new AllSelect[T]()
    case _ => new NamedSelect[T](s)
  }

  //Two hack to make this work:
  //1. No return type
  //2. For some reason the SelectFunc need _ for implicit to trigger, so we need casting at the end.
  implicit def funcToAliasSelect[T](func: SelectFunc[_]) = {
    func match {
      case a: AliasSelect[T] =>
        a
      case default =>
        new AliasSelect[T](func.asInstanceOf[SelectFunc[T]])
    }
  }


  //** Filter support **
  //  implicit def simpleFilterToComposite[T](f: FilterFunc[T]): CompositeFilterFunc[T] = new CompositeFilterFunc[T](f)
//  implicit def symbolFilter[T](s: Symbol): SymbolFilterFunc[T] = new SymbolFilterFunc[T](s.name)

  //  def ^[T](s: Symbol): SymbolFilterFunc[T] = new SymbolFilterFunc[T](s.name)

  //  def ^(name: String): SymbolFilterFunc = null
  //** Groupby support **

  //** Select functions ***
  class FuncSelect[T](f: (T) => Any) extends SelectFunc[T] {
    override def apply(v1: T): Any = f.apply(v1)
  }

  class NamedSelect[T](val name: Symbol) extends DataFactorySelectFunc[T] {
    //NOTE: the fac from DataFactorySelectFunc need to be initialized by select
    var select: SelectFunc[T] = null

    override def apply(v1: T): Any = {
      select.apply(v1)
    }

    override def validate(): Unit = {
      if (select != null) {
        throw new IllegalStateException("Select func has been validated")
      }
      select = fac.columnMap(name).select
      if (select == null) {
        throw new IllegalArgumentException("Unknown select name '%s'".format(name))
      }
    }

    def +(that: Any): SelectFunc[T] = {
      this //TODO: finish this
    }

    def >(that: Any): SelectFunc[T] = {
      this //TODO: finish this
    }
  }

  class AllSelect[T]() extends DataFactorySelectFunc[T] {
    //NOTE: the fac from DataFactorySelectFunc need to be initialized by select
    var selects: Seq[SelectFunc[T]] = null

    override def apply(v1: T): Any = {
      selects.map(_.apply(v1))
    }

    override def validate(): Unit = {
      if (selects != null) {
        throw new IllegalStateException("Select func has been validated")
      }
      selects = fac.columnMap.map(t => t._2.select).toSeq
    }
  }

  class AliasSelect[T](func: SelectFunc[T], val id: Int = scala.util.Random.nextInt) extends SelectFunc[T] {
    var name: Symbol = null

    override def apply(v1: T): Any = func.apply(v1)

    def as(alias: Symbol): AliasSelect[T] = {
      name = alias
      this
    }

    override def toString(): String = {
      id.toString
    }
  }

  //support select ("any string")
  class StringLiteralSelect[T](v: String) extends SelectFunc[T] {
    override def apply(v1: T): Any = v
  }

  //support select (1, 0.1)
  class AnyValSelect[T](v: AnyVal) extends SelectFunc[T] {
    override def apply(v1: T): Any = v
  }

  //support select (sum(_.age))
  class SumSelect[T](f: SelectFunc[T]) extends SelectFunc[T] {
    override def apply(v1: T): Any = {
      val value = f.apply(v1)
      new Aggregate[Int](value.asInstanceOf[Int], (a, b) => a + b)
    }
  }

  def sum[T](f: SelectFunc[T]) = {
    new SumSelect[T](f)
  }

  //** Filter Func **
  //a placeholder for complex filter func
  class CompositeFilterFunc[T](val first: FilterFunc[T]) extends FilterFunc[T] {
    override def apply(v1: T): Boolean = ??? //not meant to be implemented


    def and(second: FilterFunc[T]): FilterFunc[T] = {
      new AndFilterFunc[T](Seq(first, second))
    }

    def or(second: FilterFunc[T]): FilterFunc[T] = {
      new OrFilterFunc[T](Seq(first, second))
    }
  }

  class AndFilterFunc[T](val filters: Seq[FilterFunc[T]]) extends FilterFunc[T] {
    override def apply(v1: T): Boolean = {
      filters.foreach(filter => if (!filter.apply(v1)) return false)
      return true
    }
  }

  class OrFilterFunc[T](val filters: Seq[FilterFunc[T]]) extends FilterFunc[T] {
    override def apply(v1: T): Boolean = {
      filters.foreach(filter => if (filter.apply(v1)) return true)
      return false
    }
  }

  //this is just part of filter
  class SymbolFilterFunc[T](val name: String) extends FilterFunc[T] {
    var op: String = null
    var rhs: Any = null

    override def apply(v1: T): Boolean = ???

    def >(that: Any): FilterFunc[T] = new GreaterThan[T](name, that)

    def >=(that: Any): FilterFunc[T] = new GreaterThanEqual[T](name, that)

    //    def ==(that: Any): AFilterFunc[T] = new Equal(name, that)

    //    def !=(that: Any): AFilterFunc[T] = new NotEqual(name, that)

    def <(that: Any): FilterFunc[T] = new LessThan[T](name, that)

    def <=(that: Any): FilterFunc[T] = new LessThanEqual[T](name, that)
  }

  class GreaterThan[T](val name: String, val that: Any) extends FilterFunc[T] {
    override def apply(v1: T): Boolean = ???
  }


  class GreaterThanEqual[T](val name: String, val that: Any) extends FilterFunc[T] {
    override def apply(v1: T): Boolean = ???
  }

  class Equal[T](val name: String, val that: Any) extends FilterFunc[T] {
    override def apply(v1: T): Boolean = ???
  }

  class NotEqual[T](val name: String, val that: Any) extends FilterFunc[T] {
    override def apply(v1: T): Boolean = ???
  }

  class LessThan[T](val name: String, val that: Any) extends FilterFunc[T] {
    override def apply(v1: T): Boolean = ???
  }

  class LessThanEqual[T](val name: String, val that: Any) extends FilterFunc[T] {
    override def apply(v1: T): Boolean = ???
  }

}
