package tabular.expression

abstract class Expression

abstract class ExprVisitor[T] {
  def visit(expr: Expr): T

  def visit(expr: AndExpr): T

  def visit(expr: OrExpr): T

  def visit(expr: NotExpr): T

  def visit(expr: EqualExpr): T

  def visit(expr: NotEqualExpr): T

  def visit(expr: LessThanExpr): T

  def visit(expr: LessThanEqExpr): T

  def visit(expr: GreaterThanExpr): T

  def visit(expr: GreaterThanEqExpr): T

  def visit(expr: PlusExpr): T

  def visit(expr: MinusExpr): T

  def visit(expr: MultiplyExpr): T

  def visit(expr: DivideExpr): T

  def visit(expr: ModExpr): T

  def visit(expr: SymExpr): T

  def visit(expr: AnyValExpr): T

  def visit(expr: StringExpr): T

}

class DefaultExprVisitor[T] extends ExprVisitor[T] {
  override def visit(expr: Expr): T = ???

  override def visit(expr: GreaterThanExpr): T = ???

  override def visit(expr: LessThanEqExpr): T = ???

  override def visit(expr: PlusExpr): T = ???

  override def visit(expr: GreaterThanEqExpr): T = ???

  override def visit(expr: MultiplyExpr): T = ???

  override def visit(expr: MinusExpr): T = ???

  override def visit(expr: ModExpr): T = ???

  override def visit(expr: DivideExpr): T = ???

  override def visit(expr: AndExpr): T = ???

  override def visit(expr: OrExpr): T = ???

  override def visit(expr: NotExpr): T = ???

  override def visit(expr: EqualExpr): T = ???

  override def visit(expr: NotEqualExpr): T = ???

  override def visit(expr: LessThanExpr): T = ???

  override def visit(expr: SymExpr): T = ???

  override def visit(expr: AnyValExpr): T = ???

  override def visit(expr: StringExpr): T = ???
}

class ParentOf[T <: Expr](val a: T, val b: T)

trait Expr {
  def accept[T](v: ExprVisitor[T]): T
}

trait BooleanLogic extends Expr {
  def and(that: BooleanLogic): BooleanLogic = new AndExpr(this, that)

  def or(that: BooleanLogic): BooleanLogic = new OrExpr(this, that)

  def unary_!(): BooleanLogic = new NotExpr(this)

}

abstract class BooleanLogicExpr(a: BooleanLogic, b: BooleanLogic) extends ParentOf[BooleanLogic](a, b) with BooleanLogic

case class AndExpr(override val a: BooleanLogic, override val b: BooleanLogic) extends BooleanLogicExpr(a, b) {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

case class OrExpr(override val a: BooleanLogic, override val b: BooleanLogic) extends BooleanLogicExpr(a, b) {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

case class NotExpr(override val a: BooleanLogic) extends BooleanLogicExpr(a, null) {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

trait Relational extends BooleanLogic {
  def eq_(that: Relational): EqualExpr = new EqualExpr(this, that)

  def ne_(that: Relational): NotEqualExpr = new NotEqualExpr(this, that)

  def lt_(that: Relational): LessThanExpr = new LessThanExpr(this, that)

  def gt_(that: Relational): GreaterThanExpr = new GreaterThanExpr(this, that)

  def lte_(that: Relational): LessThanEqExpr = new LessThanEqExpr(this, that)

  def gte_(that: Relational): GreaterThanEqExpr = new GreaterThanEqExpr(this, that)
}

abstract class RelationalExpr(a: Relational, b: Relational) extends ParentOf[Relational](a, b) with Relational

case class EqualExpr(override val a: Relational, override val b: Relational) extends RelationalExpr(a, b) {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

case class NotEqualExpr(override val a: Relational, override val b: Relational) extends RelationalExpr(a, b) {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

case class LessThanExpr(override val a: Relational, override val b: Relational) extends RelationalExpr(a, b) {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

case class GreaterThanExpr(override val a: Relational, override val b: Relational) extends RelationalExpr(a, b) {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

case class LessThanEqExpr(override val a: Relational, override val b: Relational) extends RelationalExpr(a, b) {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

case class GreaterThanEqExpr(override val a: Relational, override val b: Relational) extends RelationalExpr(a, b) {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

trait Arithmetic extends Relational {
  def +(that: Arithmetic): PlusExpr = new PlusExpr(this, that)

  def -(that: Arithmetic): MinusExpr = new MinusExpr(this, that)

  def *(that: Arithmetic): MultiplyExpr = new MultiplyExpr(this, that)

  def /(that: Arithmetic): DivideExpr = new DivideExpr(this, that)

  def %(that: Arithmetic): ModExpr = new ModExpr(this, that)
}

abstract class ArithmeticExpr(a: Arithmetic, b: Arithmetic) extends ParentOf[Arithmetic](a, b) with Arithmetic

case class PlusExpr(override val a: Arithmetic, override val b: Arithmetic) extends ArithmeticExpr(a, b) {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

case class MinusExpr(override val a: Arithmetic, override val b: Arithmetic) extends ArithmeticExpr(a, b) {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

case class MultiplyExpr(override val a: Arithmetic, override val b: Arithmetic) extends ArithmeticExpr(a, b) {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

case class DivideExpr(override val a: Arithmetic, override val b: Arithmetic) extends ArithmeticExpr(a, b) {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

case class ModExpr(override val a: Arithmetic, override val b: Arithmetic) extends ArithmeticExpr(a, b) {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

abstract class AtomExpr extends Arithmetic

case class SymExpr(val s: Symbol) extends AtomExpr {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

case class BoolExpr(val b: Boolean) extends AtomExpr {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

case class AnyValExpr(val v: AnyVal) extends AtomExpr {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

case class StringExpr(val s: String) extends AtomExpr {
  override def accept[T](v: ExprVisitor[T]): T = v.visit(this)
}

/**
 * Created by tiong on 8/3/14.
 */
class Expresso {

}

object Expresso {
  implicit def symExpr(s: Symbol): SymExpr = {
    new SymExpr(s)
  }

  implicit def boolExpr(v: Boolean): BoolExpr = {
    new BoolExpr(v);
  }

  implicit def valExpr(v: AnyVal): AnyValExpr = {
    new AnyValExpr(v)
  }


  def main(args: Array[String]) {
    println('a + 1)
    println('a + 'b)
    println('a eq_ ('b + 1) and 'b)
    println('a ne_ ('b + 2) and 'b)
    println(!('c + 'b))
    println(!'c)



    val expr = 'a + 1 eq_ 'b and 'c or !'d
  }
}
