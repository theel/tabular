package tabular

import tabular.core.DataFactory

/**
 * Created by tiong on 7/24/14.
 */
class ReflectDataFactory[T: Manifest] extends DataFactory[T]{
  override def getColumns(): Seq[Column[T, _]] = ???

  override def getValue(value: T, s: Symbol): AnyRef = ???
}
