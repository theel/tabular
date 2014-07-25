/**
 * Created by tiong on 7/23/14.
 */

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import tabular.QuerySupport._
import tabular._

class QueryTest extends FunSpec with ShouldMatchers{

  case class Person(firstName: String, lastName: String, age: Int) {
    override def toString() = "Person(%s, %s, %d)".format(firstName, lastName, age)
    def toRow() =  Seq(firstName, lastName, age)
  }

  class PersonDF extends DataFactory[Person]{
    override def getColumns():  Seq[Column[Person, _]]  = Seq(//
      new Column[Person, String]("firstName", _.firstName),//
      new Column[Person, String]("lastName", _.lastName),//
      new Column[Person, Int]("age", _.age)//
    )

    override def getValue(value: Person, s: String): Any = ???
  }

  val data = Seq(//
    new Person("John", "Smith", 20), //
    new Person("John", "Doe", 71), //
    new Person("John", "Johnson", 5) //
  )
  val dataRows = data.map(_.toRow())

  val table = new ListTable[Person](data, new PersonDF())

  def executeAndMatch(query: Statement[Person], expected: Seq[Seq[Any]]) = {
    val results = query.compile().execute()
    val resultSeq = results.rows().toSeq
    println(resultSeq.map(row => row.map(_.getClass)))
    println(resultSeq.mkString(","))
    println(expected.map(row => row.map(_.getClass)))
    println(expected.mkString(","))
    resultSeq should equal (expected)
  }

  describe("Tabular query") {

    it("should allow identity select") {
      val query = table select (p => p)
      val expected = data.map(Seq(_)) //each row has single column of Person
      executeAndMatch(query, expected)
    }

    it("should allow select with func") {
      val query = table select (_.firstName, _.age > 65)
      val expected = data.map(p => Seq(p.firstName, p.age > 65))
      executeAndMatch(query, expected)
    }

    it("should allow select with literals") {
      val query = table select(_.firstName, "test", 1)
      val expected = data.map(p => Seq(p.firstName, "test", 1))
      executeAndMatch(query, expected)
    }

    it("should allow select with symbol") {
      val query = table select('firstName, 'lastName)
      val expected = data.map(p => Seq(p.firstName, p.lastName))
      executeAndMatch(query, expected)
    }



    it("select with filter") {
      val query = table select(_.firstName, _.lastName, _.age > 65, 1) where (p => p.firstName == "John" && p.lastName == "Doe")
      //    dump(query4.compile.execute())
    }

    it("group by and order") {
      val query5 = table select(_.firstName, sum[Person](_.age)) groupBy (_.firstName) having (_.age > 20) orderBy (_.firstName)
      //      dump(query5.compile.execute())
    }
    //Select and group
    it("group by") {
      val query6 = table select(_.firstName, _.lastName, sum[Person](_.age)) groupBy (_.lastName)
      //    dump(query6.compile.execute())
    }
  }
}
