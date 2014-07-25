package tabular

import tabular.Tabular._

/**
 * Created by tiong on 7/24/14.
 */
object QuerySupport {

  //** Select support **
  // Func conversion
  implicit def funcSelect[T](v: (T) => Any): SelectFunc[T] = new FuncSelectFunc[T](v)

  // Literal conversion
  implicit def stringSelect[T](v: String): SelectFunc[T] = new StringLiteralSelect[T](v)
  implicit def anyValSelect[T](v: AnyVal): SelectFunc[T] = new AnyValSelect[T](v)

  // Symbol conversion
  implicit def symbolSelect[T](s: Symbol): SelectFunc[T] = s.name match {
    //    case "*" => new AllSelect[T]()
    case _ => new NamedSelect[T](s.name)
  }

  class FuncSelectFunc[T](f: (T) => Any) extends SelectFunc[T]() {
    println("Converted to func")
    override def apply(v1: T): Any = f.apply(v1)
  }

  class NamedSelect[T](name: String) extends SelectFunc[T] {
    override def apply(v1: T): Any = ???
  }

  class FuncSelect[T](f: T => Any) extends SelectFunc[T] {
    override def apply(v1: T): Any = ???
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

  def sum[T](f: SelectFunc[T]) = {
    new SumSelect[T](f)
  }
}
