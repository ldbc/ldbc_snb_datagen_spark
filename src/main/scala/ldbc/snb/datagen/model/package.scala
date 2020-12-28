package ldbc.snb.datagen

import shapeless.lens

package object model {
  sealed trait Cardinality
  object Cardinality {
    case object OneN extends Cardinality
    case object NN extends Cardinality
  }

  sealed trait EntityType {
    val isStatic: Boolean
  }
  object EntityType {
    final case class Node(name: String, isStatic: Boolean = false) extends EntityType

    final case class Edge(
      `type`: String,
      source: String,
      destination: String,
      cardinality: Cardinality,
      isStatic: Boolean = false
    ) extends EntityType

    final case class Attr(`type`: String, parent: String, attribute: String, isStatic: Boolean = false) extends EntityType
  }

  object EntityTypeLens {
    private val isStaticLens = lens[EntityType].isStatic

    def static(entity: EntityType): Boolean = isStaticLens.get(entity).contains(true)
  }

  case class Graph[D](
    layout: String,
    entities: Map[EntityType, D]
  )

  sealed trait BatchPeriod
  object BatchPeriod {
    case object Day extends BatchPeriod
    case object Month extends BatchPeriod
  }
}
