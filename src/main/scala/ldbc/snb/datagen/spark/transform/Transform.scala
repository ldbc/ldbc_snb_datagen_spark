package ldbc.snb.datagen.spark.transform

import ldbc.snb.datagen.spark.model.DataFrameGraph

trait Transform {
  def transform(input: DataFrameGraph): DataFrameGraph
}


