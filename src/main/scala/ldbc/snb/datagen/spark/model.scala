package ldbc.snb.datagen.spark

import org.apache.spark.sql.DataFrame

object model {

  sealed trait Cardinality
  object Cardinality {
    case object N1 extends Cardinality
    case object NN extends Cardinality
  }

  sealed trait Entity {
    def dataset: DataFrame
    def isStatic: Boolean
  }
  object Entity {
    final case class Node(dataset: DataFrame, isStatic: Boolean) extends Entity

    final case class Edge(
      dataset: DataFrame,
      isStatic: Boolean,
      source: String,
      destination: String,
      cardinality: Cardinality
    ) extends Entity

    final case class Attr(dataset: DataFrame, isStatic: Boolean, ref: String) extends Entity
  }

  case class Graph(entities: Map[String, Entity])
}
