package tabular

import tabular.expression._

/**
 * Created by tiong on 8/5/14.
 */
 class TreeVisitor[T] extends ExprVisitor[T]{
  override def visit(e: Expr): T = ???

  override def visit(e: OrExpr): T = ???

  override def visit(e: AndExpr): T = ???

  override def visit(e: DivideExpr): T = ???

  override def visit(e: ModExpr): T = ???

  override def visit(e: LessThanEqExpr): T = ???

  override def visit(e: LessThanExpr): T = ???

  override def visit(e: NotEqualExpr): T = ???

  override def visit(e: EqualExpr): T = ???

  override def visit(e: NotExpr): T = ???

  override def visit(e: MultiplyExpr): T = ???

  override def visit(e: MinusExpr): T = ???

  override def visit(e: PlusExpr): T = ???

  override def visit(e: GreaterThanEqExpr): T = ???

  override def visit(e: GreaterThanExpr): T = ???

  override def visit(e: StringExpr): T = ???

  override def visit(e: AnyValExpr): T = ???

  override def visit(e: SymExpr): T = ???
}
