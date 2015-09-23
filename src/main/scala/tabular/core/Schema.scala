package tabular.core

/**
 * Represent a column
 */
abstract class Column[T: Manifest](val name: Symbol) {
  type D

//  def +(other: AnyRef) = ???
//  def +(other: Column[T]) = ???
//  def +(other: AnyRef) = ???


}

abstract class Schema[T: Manifest] {

  //this must be lazy so the fields can be discovered through reflection
  lazy val columns: Seq[Column[T]] = {
    //use reflection to get all the fields
    getClass.getMethods.flatMap {
      method =>
        val returnType = method.getReturnType
        if (method.getDeclaringClass != classOf[Schema[T]] && classOf[Column[_]].isAssignableFrom(returnType)) {
          val col = method.invoke(this).asInstanceOf[Column[T]]
          println("Returning " + col)
          Seq(col)
        } else {
          Seq()
        }
    }
  }

  lazy val columnMap: Map[Symbol, Column[T]] = columns.map { c => (c.name, c)}.toMap

  protected[Schema] def int(name: Symbol) = new IntColumn[Manifest[T]](name)

  protected[Schema] def long(name: Symbol) = new LongColumn[T](name)

  protected[Schema] def float(name: Symbol) = new FloatColumn[T](name)

  protected[Schema] def double(name: Symbol) = new DoubleColumn[T](name)

  protected[Schema] def string(name: Symbol) = new StringColumn[T](name)

  protected[Schema] def byte(name: Symbol) = new ByteColumn[T](name)

  protected[Schema] def bool(name: Symbol) = new ByteColumn[T](name)
}

object Schema {
}

class IntColumn[T: Manifest](name: Symbol) extends Column(name) {
  type D = Int
}

class LongColumn[T: Manifest](name: Symbol) extends Column(name) {
  type D = Long
}


class FloatColumn[T: Manifest](name: Symbol) extends Column(name) {
  type D = Float
}

class DoubleColumn[T: Manifest](name: Symbol) extends Column(name) {
  type D = Double
}

class StringColumn[T: Manifest](name: Symbol) extends Column(name) {
  type D = String
}

class ByteColumn[T: Manifest](name: Symbol) extends Column(name) {
  type D = Byte
}

class BooleanColumn[T: Manifest](name: Symbol) extends Column(name) {
  type D = Boolean
}
