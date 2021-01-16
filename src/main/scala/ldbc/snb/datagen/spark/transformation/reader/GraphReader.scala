package ldbc.snb.datagen.spark.transformation.reader

import ldbc.snb.datagen.spark.transformation.model.{Graph, GraphDef}

trait GraphReader[D] {
  def read(definition: GraphDef, path: String): Graph[D]
}




