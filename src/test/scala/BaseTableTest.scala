import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import tabular.core.QuerySupport._
import tabular.core.{Statement, Table}
import tabular.util.RowOrdering


case class Person(id: Int, firstName: String, lastName: String, age: Int, spouseId: Int) {
  override def toString() = "Person(%s, %s, %d)".format(firstName, lastName, age)

  def toRow() = Seq(firstName, lastName, age)
}


abstract class BaseTableTest(testName: String, table: Table[Person], data: Seq[Person]) extends FunSuite with ShouldMatchers {

  test(testName + ": should allow identity select") {
    val query1 = table select(p => p, (p: Person) => p)
    val expected1 = data.map(p => (Seq(p, p))) //each row has single column of Person
    executeAndMatch(query1, expected1, Seq("field0"))

    val query2 = table select ((p: Person) => p) //this uses implicit
    val expected2 = data.map(p => (Seq(p))) //each row has single column of Person
    executeAndMatch(query2, expected2, Seq("field0"))
  }

  test(testName + ": should allow select with func") {
    val query1 = table select(_.firstName, _.age > 65)
    val expected1 = data.map(p => Seq(p.firstName, p.age > 65))
    executeAndMatch(query1, expected1, Seq("firstName", "field1"))

    //      val query2 = table select ('name, 'age > 65)
    //      executeAndMatch(query2, expected1, Seq("firstName", "field1"))
  }

  test(testName + ": should allow select with literals") {
    val query1 = table select(testName + ": test", 1, 1 + 1)
    val expected1 = data.map(p => Seq(testName + ": test", 1, 2))
    executeAndMatch(query1, expected1, Seq(testName + ": field0", "field1", "field2"))
  }

  test(testName + ": should allow select with symbol") {
    val query1 = table select('firstName, 'lastName)
    val expected1 = data.map(p => Seq(p.firstName, p.lastName))
    executeAndMatch(query1, expected1, Seq(testName + ": firstName", "lastName"))
  }

  test(testName + ": should allow select *") {
    val query1 = table select ('*) //need to support multi-select
    val expected1 = data.map(p => Seq(Seq(p.id, p.firstName, p.lastName, p.age, p.spouseId))) //TODO: need to flatten
    executeAndMatch(query1, expected1, Seq(testName + ": id", "firstName", "lastName", "age", "spouseId"))
  }

  test(testName + ": should allow support aliasing") {
    val fullname = (p: Person) => "%s, %s".format(p.lastName, p.firstName)
    val query1 = table select (fullname as 'fullName)
    val expected1 = data.map(fullname).map(Seq(_)) //each row has single column of Person
    executeAndMatch(query1, expected1, Seq(testName + ": fullName"))
  }

  test(testName + ": should select with filter func") {
    val query1 = table select(_.firstName, _.lastName, _.age) where_ (_.age > 65)
    val expected1 = data.filter(_.age > 65).map(p => Seq(p.firstName, p.lastName, p.age))
    executeAndMatch(query1, expected1, Seq(testName + ": field0", "field1", "field2"))
  }

  test(testName + ": should select with complex filter func") {
    val filterFunc = (p: Person) => (p.firstName == "John" && p.age > 65) || p.lastName == "Smith"
    val query1 = table select(_.firstName, _.lastName, _.age) where_ (filterFunc)
    val expected1 = data.filter(filterFunc).map(p => Seq(p.firstName, p.lastName, p.age))
    executeAndMatch(query1, expected1, Seq(testName + ": field0", "field1", "field2"))
  }

  test(testName + ": should select with multiple filter funcs") {
    val query1 = table select(_.firstName, _.lastName, _.age) where_(_.firstName == "John", _.age > 65)
    val expected1 = data.filter(p => p.firstName == "John" && p.age > 65).map(p => Seq(p.firstName, p.lastName, p.age))
    executeAndMatch(query1, expected1, Seq(testName + ": field0", "field1", "field2"))
  }

  test(testName + ": should do order-by") {
    val query1 = table select ('firstName) orderBy ('firstName)
    val expected1 = data.map(_.firstName).sorted.map(Seq(_))
    executeAndMatch(query1, expected1, Seq(testName + ": firstName"))
  }

  test(testName + ": should group by simple group") {
    val query1 = table select ('firstName) groupBy ('firstName) orderBy ('firstName)
    val expected1 = data.map(_.firstName).sorted.distinct.map(Seq(_))
    executeAndMatch(query1, expected1, Seq(testName + ": firstName"))
  }

  test(testName + ": should group by and aggregate with aggregate function") {
    val query1 = table select('firstName, sum[Person](_.age)) groupBy ('firstName) orderBy ('firstName)
    val grouped = data.map(p => (p.firstName, p.age)).groupBy(_._1).map(_._2) //Seq[(firstName, age)
    val expected1 = grouped.map(_.reduce((a, b) => (a._1, a._2 + b._2))).map(t => Seq(t._1, t._2)).toSeq.sorted(RowOrdering)
    executeAndMatch(query1, expected1, Seq(testName + ": firstName", "field1"))
  }


  test(testName + ": should group by and aggregate with aggregate function supporting symbol") {
    val query1 = table select('firstName, sum[Person]('age)) groupBy ('firstName) orderBy ('firstName)
    val grouped = data.map(p => (p.firstName, p.age)).groupBy(_._1).map(_._2) //Seq[(firstName, age)
    val expected1 = grouped.map(_.reduce((a, b) => (a._1, a._2 + b._2))).map(t => Seq(t._1, t._2)).toSeq.sorted(RowOrdering)
    executeAndMatch(query1, expected1, Seq(testName + ": firstName", "field1"))
  }

  test(testName + ": should group by and aggregate with simple value") {
    val query1 = table select('firstName, 'lastName, sum[Person](_.age)) groupBy ('firstName) orderBy ('firstName)
    val grouped = data.map(p => (p.firstName, p.lastName, p.age)).groupBy(_._1).map(_._2) //Seq[(firstName, lastName, age)
    val expected1 = grouped.map(_.reduce((a, b) => (a._1, a._2, a._3 + b._3))).map(t => Seq(t._1, t._2, t._3)).toSeq
    val expected2 = expected1.sorted(RowOrdering)
    executeAndMatch(query1, expected2, Seq("firstName", "field1"))
  }

  //      it("should be able to do full-join") {
  //        val query1 = table join table select(_._1.firstName, _._2.firstName)
  //        val names = data.map(p => (p.firstName))
  //        val expected1 = names.flatMap {
  //          name => names.map(Seq(name, _))
  //        }
  //        executeAndMatch(query1, expected1, Seq("firstName", "field1"))
  //      }
  //
  //      it("should be able to do join on") {
  //        val query1 = table join table on(_.firstName, _.firstName) select(_._1.firstName, _._2.firstName)
  //        val names = data.map(p => (p.firstName))
  //        val expected1 = names.flatMap {
  //          name1 => names.flatMap { name2 => if (name1 == name2) Seq(Seq(name1, name2)) else Seq()}
  //        }
  //        executeAndMatch(query1, expected1, Seq("firstName", "field1"))
  //      }

  //  }


  def executeAndMatch(query: Statement[_], expected: Seq[Seq[Any]], columns: Seq[String]) = {
    try {
      //      println("")
      val results = query.compile()
      results.dataFac.getColumns()
      val resultSeq = results.rows().toSeq
      //      println("==== Results =========")
      //      //    println(resultSeq.map(row => row.map(_.getClass)))
      //      println(resultSeq.map(_.mkString(",")).mkString("\n"))
      //      println("")
      //      println("==== expected1 ====")
      //      //    println(expected.map(row => row.map(_.getClass)))
      //      println(expected.map(_.mkString(",")).mkString("\n"))
      resultSeq should equal(expected)
    } catch {
      case e =>
        e.printStackTrace()
        throw e
    }
  }


}