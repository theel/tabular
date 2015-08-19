import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import tabular.ExprStructPrinter
import tabular.expression._
import Expresso._
import tabular._

/**
 * Created by tiong on 8/3/14.
 */
class ExpressoTest extends FunSpec with ShouldMatchers {

  val printer = new ExprStructPrinter()
  describe("Expresso") {
    it("should support symbol as arithmetic") {
      'sym + 1 should equal(new PlusExpr(new SymExpr('sym), new AnyValExpr(1)))
      'sym - 1 should equal(new MinusExpr(new SymExpr('sym), new AnyValExpr(1)))
      'sym * 1 should equal(new MultiplyExpr(new SymExpr('sym), new AnyValExpr(1)))
      'sym / 1 should equal(new DivideExpr(new SymExpr('sym), new AnyValExpr(1)))
      'sym % 1 should equal(new ModExpr(new SymExpr('sym), new AnyValExpr(1)))
    }
    it("should support symbol as relational") {
      'sym eq_ 1 should equal(new EqualExpr(new SymExpr('sym), new AnyValExpr(1)))
      'sym ne_ 1 should equal(new NotEqualExpr(new SymExpr('sym), new AnyValExpr(1)))
      'sym gt_ 1 should equal(new GreaterThanExpr(new SymExpr('sym), new AnyValExpr(1)))
      'sym gte_ 1 should equal(new GreaterThanEqExpr(new SymExpr('sym), new AnyValExpr(1)))
      'sym lt_ 1 should equal(new LessThanExpr(new SymExpr('sym), new AnyValExpr(1)))
      'sym lte_ 1 should equal(new LessThanEqExpr(new SymExpr('sym), new AnyValExpr(1)))
    }
    it("should support symbol as booleanlogic") {
      'sym and true should equal(new AndExpr(new SymExpr('sym), new BoolExpr(true)))
      'sym or true should equal(new OrExpr(new SymExpr('sym), new BoolExpr(true)))
      !'sym should equal(new NotExpr(new SymExpr('sym)))
      'sym and true or 'sym
    }
    it("should support precedence (due to scala)") {
      'sym + 'sym - 1 + 'sym * 'sym should equal(new PlusExpr(new MinusExpr(new PlusExpr(new SymExpr('sym), new SymExpr('sym)), new AnyValExpr(1)), new MultiplyExpr(new SymExpr('sym), new SymExpr('sym))))
    }

    it("negative test") {

      //#1: can't case arithmetic to boolean
      //      val a = 'symc + 1 and true //can't use arithmetic in boolean
      //#2: can't case arithmetic to relation
      //      val c = ('sym + 1 eq_ 'sym) + 2 //can't do arithmetic in relational
      //#3: can't case relational to boolean
      //      val b = 'a eq_ 'b eq_ d //can't chain relational comparison
    }
  }

  it("should a") {
    //   (sym/(sum+1-1)/sym*
  }


}
