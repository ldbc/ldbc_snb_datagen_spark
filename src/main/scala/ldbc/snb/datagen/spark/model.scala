package ldbc.snb.datagen.spark

import org.apache.spark.sql.DataFrame
import shapeless._

object model {

  sealed trait Cardinality
  object Cardinality {
    case object N1 extends Cardinality
    case object NN extends Cardinality
  }

  sealed trait Entity {
    val dataset: DataFrame
    val isStatic: Boolean
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

    private val datasetLens = lens[Entity].dataset
    private val isStaticLens = lens[Entity].isStatic

    def static(entity: Entity): Boolean = isStaticLens.get(entity).contains(true)

    def transformed(entity: Entity, f: DataFrame => DataFrame): Entity = datasetLens.modify(entity)(f)
  }

  case class Graph(entities: Map[String, Entity])

  sealed trait BatchPeriod
  object BatchPeriod {
    case object Day extends BatchPeriod
    case object Month extends BatchPeriod
  }
}
