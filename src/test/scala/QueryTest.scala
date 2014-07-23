/**
 * Created by tiong on 7/23/14.
 */

import org.scalatest.FunSpec
import tabular.ListTable
import tabular.Tabular._

class QueryTest extends FunSpec {

  case class Person(firstName: String, lastName: String, age: Int) {
    override def toString() = "firstName=" + firstName + ",lastName=" + lastName + ",age=" + age
  }

  val data = Seq(//
    new Person("John", "Smith", 20), //
    new Person("John", "Doe", 71), //
    new Person("John", "Johnson", 5) //
  )
  val table = new ListTable[Person](data)

  describe("It should fulfill different kind of queries correctly") {

    it("identity select") {
      val query = table select (p => p)
    }
    it("simple field select") {
      val query = table select (_.firstName)
      //      dump(query2.compile.execute())
    }

    it("select with func and literal") {
      val query = table select(_.firstName, _.lastName, _.age > 65, 1)
      //      dump(query3.compile.execute())
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
