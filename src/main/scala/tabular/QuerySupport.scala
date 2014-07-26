package tabular

import tabular.Tabular._

/**
 * Created by tiong on 7/24/14.
 */
object QuerySupport {

  //** Select support **
  // Func conversion
//  implicit def funcSelect[T](v: (T) => Any): SelectFunc[T] = new FuncSelectFunc[T](v)

  // Literal conversion
  implicit def stringSelect[T](v: String): SelectFunc[T] = new StringLiteralSelect[T](v)
  implicit def anyValSelect[T](v: AnyVal): SelectFunc[T] = new AnyValSelect[T](v)

  // Symbol conversion
  implicit def symbolSelect[T](s: Symbol): SelectFunc[T] = s.name match {
    case "*" => new AllSelect[T]()
    case _ => new NamedSelect[T](s.name)
  }

  //** Filter **
  implicit def simpleFilterToComposite[T](f: FilterFunc[T]): CompositeFilterFunc[T] = new CompositeFilterFunc[T](f)

  //** Select functions ***
  class FuncSelectFunc[T](f: (T) => Any) extends SelectFunc[T]() {
    println("Converted to func")
    override def apply(v1: T): Any = f.apply(v1)
  }

  class NamedSelect[T](name: String) extends DataFactorySelectFunc[T] {
    //NOTE: the fac from DataFactorySelectFunc need to be initialized by select
    var select: SelectFunc[T] = null
    override def apply(v1: T): Any = {
      select.apply(v1)
    }

    override def validate(): Unit = {
      if (select!=null){
        throw new IllegalStateException("Select func has been validated")
      }
      select = fac.columnMap(name).select
      if (select==null){
        throw new IllegalArgumentException("Unknown select name '%s'".format(name))
      }

    }
  }

  class AllSelect[T]() extends DataFactorySelectFunc[T] {
    //NOTE: the fac from DataFactorySelectFunc need to be initialized by select
    var selects: Seq[SelectFunc[T]] = null
    override def apply(v1: T): Any = {
      selects.map(_.apply(v1))
    }

    override def validate(): Unit = {
      if (selects!=null){
        throw new IllegalStateException("Select func has been validated")
      }
      selects = fac.columnMap.map(t => t._2.select).toSeq
    }
  }

  //support select ('firstName)
  class SymbolSelect[T](name: String) extends SelectFunc[T] {
    override def apply(v1: T): Any = ???
  }


  class StringLiteralSelect[T](v: String) extends SelectFunc[T] {
    override def apply(v1: T): Any = v
  }
  //support select ("any string", 1, 0.1)
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
  def sum[T](f: SelectFunc[T]) = {
    new SumSelect[T](f)
  }
}
