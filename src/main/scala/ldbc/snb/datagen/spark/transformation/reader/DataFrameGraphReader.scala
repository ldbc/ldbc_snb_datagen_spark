package ldbc.snb.datagen.spark.transformation.reader
import better.files._

import ldbc.snb.datagen.spark.transformation.model.{Graph, GraphDef}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataFrameGraphReader(implicit spark: SparkSession) extends GraphReader[DataFrame] {
  override def read(definition: GraphDef, path: String): Graph[DataFrame] = {
    val entities = (for { entity <- definition.entities } yield {
      val df =spark.read.options(Map("header" -> "true", "separator" -> "|")).csv((path / entity.entityPath).toString())
      entity -> df
    }).toMap
    Graph(definition.layout, entities)
  }
}