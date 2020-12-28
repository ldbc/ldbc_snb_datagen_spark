package ldbc.snb.datagen.spark.transform

import ldbc.snb.datagen.model.Cardinality._
import ldbc.snb.datagen.model.EntityType._
import ldbc.snb.datagen.model.{EntityTypeLens, Graph}
import ldbc.snb.datagen.spark.model.DataFrameGraph
import org.apache.spark.sql.DataFrame

trait Transform {
  def transform(input: DataFrameGraph): DataFrameGraph
}


