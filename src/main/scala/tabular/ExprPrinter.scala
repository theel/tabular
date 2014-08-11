package tabular

/**
 * Created by tiong on 8/3/14.
 */
class ExprPrinter extends DefaultExprVisitor[String] {

  def print(expr: Expr) {
    val result = expr.accept(this)
    println(result)
  }

  override def visit(expr: Expr) = ???

  override def visit(expr: GreaterThanExpr) = formatStr(expr, "%s > %s", expr.a, expr.b)

  override def visit(expr: LessThanEqExpr) = formatStr(expr, "%s < %s", expr.a, expr.b)

  override def visit(expr: PlusExpr) = formatStr(expr, "%s + %s", expr.a, expr.b)

  override def visit(expr: GreaterThanEqExpr) = formatStr(expr, "%s >= %s", expr.a, expr.b)

  override def visit(expr: MultiplyExpr) = formatStr(expr, "%s * %s", expr.a, expr.b)

  override def visit(expr: MinusExpr) = formatStr(expr, "%s - %s", expr.a, expr.b)

  override def visit(expr: ModExpr) = formatStr(expr, "%s % %s", expr.a, expr.b)

  override def visit(expr: DivideExpr) = formatStr(expr, "%s / %s", expr.a, expr.b)

  override def visit(expr: AndExpr) = formatStr(expr, "%s and %s", expr.a, expr.b)

  override def visit(expr: OrExpr) = formatStr(expr, "%s or %s", expr.a, expr.b)

  override def visit(expr: NotExpr) = formatStr(expr, "not %s", expr.a, expr.b)

  override def visit(expr: EqualExpr) = formatStr(expr, "%==%s", expr.a, expr.b)

  override def visit(expr: NotEqualExpr) = formatStr(expr, "%s!=%s", expr.a, expr.b)

  override def visit(expr: LessThanExpr) = formatStr(expr, "%s < %s", expr.a, expr.b)

  override def visit(expr: SymExpr) = expr.s.name

  override def visit(expr: AnyValExpr) = expr.v.toString

  override def visit(expr: StringExpr) = super.visit(expr)

  def formatStr(parent: Expr, spec: String, a: Expr, b: Expr): String = {
    spec.format(a.accept(this), b.accept(this))
  }

}
