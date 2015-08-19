package tabular

import tabular.expression.Expr

/**
 * Created by tiong on 8/3/14.
 */
class ExprStructPrinter[String] extends ExprPrinter {
  override def formatStr(parent: Expr, spec: Predef.String, a: Expr, b: Expr): Predef.String = {
    return parent.getClass().getName + "(%s)".format(super.formatStr(parent, spec, a, b))
  }
}
