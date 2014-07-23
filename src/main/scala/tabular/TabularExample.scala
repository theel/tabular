package tabular

import tabular.Tabular.{anyValSelect, sum}

//import tabular.Tabular.funcSelect

object TabularExample {

  case class Person(firstName: String, lastName: String, age: Int) {
    override def toString() = "firstName=" + firstName + ",lastName=" + lastName + ",age=" + age
  }

  def dump[T](t: Table[T]): Unit = {
    println(t.rows.mkString(",\n"))
  }

  def main(args: Array[String]) {
    //Setup
    val data = Seq(//
      new Person("John", "Smith", 20), //
      new Person("John", "Doe", 71), //
      new Person("John", "Johnson", 5) //
    )

    val table = new ListTable[Person](data)
    dump(table)

    //Select all
    val query1 = table select (p => p)

    //simple select
    val query2 = table select (_.firstName)
    dump(query2.compile.execute())

    //Select with func, literal
    val query3 = table select(_.firstName, _.lastName, _.age > 65, 1)
    dump(query3.compile.execute())

    //Select with filter
    val query4 = table select(_.firstName, _.lastName, _.age > 65, 1) where (p => p.firstName == "John" && p.lastName == "Doe")
    dump(query4.compile.execute())

    //Select and group
    val query5 = table select(_.firstName, sum[Person](_.age)) groupBy (_.firstName) having (_.age > 20) orderBy (_.firstName)
    dump(query5.compile.execute())

    //Select and group
    val query6 = table select(_.firstName, _.lastName, sum[Person](_.age)) groupBy (_.lastName)
    dump(query6.compile.execute())
  }

}
