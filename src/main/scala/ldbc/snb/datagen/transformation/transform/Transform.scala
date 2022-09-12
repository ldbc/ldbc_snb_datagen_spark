package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.model.{Graph, Mode}
import ldbc.snb.datagen.transformation.transform.Transform.DataFrameGraph

trait Transform[M1 <: Mode, M2 <: Mode]
    extends (DataFrameGraph[M1] => DataFrameGraph[M2]) {
  type In  = DataFrameGraph[M1]
  type Out = DataFrameGraph[M2]
  def transform(input: In): Out
  override def apply(v1: DataFrameGraph[M1]): DataFrameGraph[M2] = transform(v1)
}

object Transform {
  type DataFrameGraph[M <: Mode] = Graph[M]
}
