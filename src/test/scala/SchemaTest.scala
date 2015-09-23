package tabular

/**
 * Created by tiong on 7/23/14.
 */

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import tabular.core._

class SchemaTest extends FunSpec with ShouldMatchers {

  case class Person(id: Int, firstName: String, lastName: String, age: Int, spouseId: Int) {
    override def toString() = "Person(%s, %s, %d)".format(firstName, lastName, age)

    def toRow() = Seq(firstName, lastName, age)
  }

  case class PersonSchema() extends Schema[Person] {
    val firstName = string('firstName)
    val lastName = string('lastName)
    val age = int('age)
  }

  val data = Seq(//
    new Person(0, "John", "Smith", 20, 4), //
    new Person(1, "John", "Doe", 71, 5), //
    new Person(2, "John", "Johnson", 5, -1), //
    new Person(3, "Adam", "Smith", 10, -1), //
    new Person(4, "Ann", "Smith", 10, 0), //
    new Person(4, "Anna", "Doe", 10, 1) //
  )
  val dataRows = data.map(_.toRow())

//  val table = new ListTable[Person](data, new PersonDF())


  describe("SchemaTest query") {

    it("should allow identity select") {
      println(new PersonSchema().columnMap)
    }

    //    it("should group by and aggregate with simple value") {
    //      val query1 = table select('firstName, 'lastName, sum[Person](_.age)) groupBy ('firstName)
    //      val grouped = data.map(p => (p.firstName, p.lastName, p.age)).groupBy(_._1).map(_._2) //Seq[(firstName, lastName, age)
    //      val expected1 = grouped.map(_.reduce((a, b) => (a._1, a._2, a._3 + b._3))).map(t => Seq(t._1, t._2, t._3)).toSeq
    //      executeAndMatch(query1, expected1, Seq("firstName", "field1"))
    //    }

    //    it("should group by, aggregate and filter with having") {
    ////      val query1 = table select('firstName, sum[Person](_.age)) groupBy ('firstName) having (^[Person]('age).>(0))
    //      val query1 = table select('firstName, sum[Person](_.age)) groupBy ('firstName) having ('firstName > 0)
    //      val grouped  = data.map(p => (p.firstName, p.age)).groupBy(_._1).map(_._2) //Seq[(firstName, age)
    //      val expected1 = grouped.map(_.reduce((a, b) => (a._1, a._2 + b._2))).map(t => Seq(t._1, t._2)).toSeq
    //      executeAndMatch(query1, expected1, Seq("firstName", "field1"))
    //    }
  }
}
