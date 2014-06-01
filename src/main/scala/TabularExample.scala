import tabular.ListTable
import tabular.Tabular.symbolSelect
import tabular.Tabular.funcSelect
import tabular.Tabular.anyValSelect
import tabular.Tabular.symbolToFilterLHS

class TabularExample {
  def main(args: Array[String]) {
    case class Person(firstName: String, lastName: String, age: Int)

    //Setup
    val data = Seq(new Person("John", "Smith", 20), new Person("John", "Doe", 71), new Person("John", "Johnson", 5))
    val table = new ListTable[Person](data)

    //Select all
    val query1 = table select ('*)

    //Select with symbol, index, func, literal
    val isSenior = funcSelect[Person](_.age>65)
    val query2 = table select ('firstName, 'lastName, isSenior, 1) where (_.firstName=="a")


  }
}
