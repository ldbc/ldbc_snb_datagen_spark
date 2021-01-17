package ldbc.snb.datagen.spark.transformation.reader
import better.files._
import ldbc.snb.datagen.spark.transformation.model.{Graph, GraphDef, Mode}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataFrameGraphReader(implicit spark: SparkSession) extends GraphReader[DataFrame] {
  val csvOptions = Map(
    "header" -> "true",
    "sep" -> "|"
  )

  override def read(definition: GraphDef, path: String): Graph[DataFrame] = {
    val entities = (for { entity <- definition.entities } yield {
      val df = spark.read.options(csvOptions).csv((path / entity.entityPath).toString())
      entity -> df
    }).toMap
    Graph(definition.layout, Mode.Raw, entities)
  }
}