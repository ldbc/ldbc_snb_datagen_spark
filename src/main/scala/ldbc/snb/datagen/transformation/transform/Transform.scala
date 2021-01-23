package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.transformation.model.{EntityType, Graph, Mode}
import org.apache.spark.sql.DataFrame

trait Transform[G1, G2] {
  def transform(input: G1): G2
}


object Transform {
  type DataFrameGraph[A <: Mode] = Graph[A, DataFrame]

  type Untyped[A1 <: Mode, A2 <: Mode] = Transform[DataFrameGraph[A1], DataFrameGraph[A2]]


  def biGraph(layout: String, entities: Map[EntityType, Mode.BI.Layout[DataFrame]]): DataFrameGraph[Mode.BI.type] = {
    Graph[Mode.BI.type, DataFrame](layout, entities)
  }
}


