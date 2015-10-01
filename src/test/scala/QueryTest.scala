/**
 * Created by tiong on 7/23/14.
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import tabular.core.DataFactory
import tabular.list.ListTable
import tabular.spark.RddTable




class QueryTest extends FunSpec with BeforeAndAfterAll {


  class PersonDF extends DataFactory[Person] {
    override def getColumns(): Seq[Column[Person, _]] = Seq(//
      new Column[Person, Int]("id", _.id),
      new Column[Person, String]("firstName", _.firstName), //
      new Column[Person, String]("lastName", _.lastName), //
      new Column[Person, Int]("age", _.age), //
      new Column[Person, Int]("spouseId", _.spouseId)
    )

    override def getValue(value: Person, s: Symbol): Any = ???
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

  val listTable = new ListTable[Person](data, new PersonDF())

  val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("test"))
  val rdd = sc.parallelize[Person](data)
  val rddTable = new RddTable[Person](rdd, new PersonDF())


  object RDDTest$Base$$ extends BaseTableTest("RDDTable", rddTable, data)

  describe("Testing list implementation") {
    new BaseTableTest("ListTable", listTable, data).execute()

  }

  describe("Testing rdd implementation") {
    RDDTest$Base$$.execute()
//    new RDDTest.registerTests("RddTable", rddTable, data)
  }

//  TableCommonTest.execute()

  override protected def afterAll(): Unit = {
    sc.stop()
    System.exit(0)
  }
}


