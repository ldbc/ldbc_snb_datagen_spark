package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.transformation.model.{Graph, Mode}
import ldbc.snb.datagen.transformation.transform.Transform.DataFrameGraph
import org.apache.spark.sql.DataFrame

trait Transform[M1 <: Mode, M2 <: Mode] {
  type In = DataFrameGraph[M1]
  type Out = DataFrameGraph[M2]
  def transform(input: In): Out
}


object Transform {
  type DataFrameGraph[M <: Mode] = Graph[M, DataFrame]
}


