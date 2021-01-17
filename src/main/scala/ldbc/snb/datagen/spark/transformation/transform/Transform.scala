package ldbc.snb.datagen.spark.transformation.transform

import ldbc.snb.datagen.spark.transformation.model.Graph

trait Transform[A, B] {
  def transform(input: Graph[A]): Graph[B]
}

object Transform {
  type Uni[A] = Transform[A, A]
}


