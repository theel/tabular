package tabular.util

/**
 * Created by tiong on 8/20/15.
 */
object Utils {
  def getIndices(fieldNames: List[Symbol], names: Seq[Symbol]): Seq[Int] = {
    val indices = names.map(symbol => fieldNames.indexOf(symbol))
    val notFoundIndices = indices.zipWithIndex.filter(_._1 == -1).map(_._2) //find anything that doesn't have mapping in select
    if (notFoundIndices.size > 0) {
      val fieldNames = notFoundIndices.map(names(_))
      throw new IllegalArgumentException("Unknown fields %s. Possible fields = %s".format(fieldNames.mkString(","), names.mkString(",")))
    }
    indices
  }
}
