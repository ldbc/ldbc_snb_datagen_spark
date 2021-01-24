package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.transformation.model.{Graph, Mode}
import org.apache.spark.sql.DataFrame

trait Transform[G1, G2] {
  def transform(input: G1): G2
}


object Transform {
  type DataFrameGraph[M <: Mode] = Graph[M, DataFrame]

  type Untyped[A1 <: Mode, A2 <: Mode] = Transform[DataFrameGraph[A1], DataFrameGraph[A2]]
}


