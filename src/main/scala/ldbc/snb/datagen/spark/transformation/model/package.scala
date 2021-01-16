package ldbc.snb.datagen.spark.transformation

import ldbc.snb.datagen.spark.util.Utils.camel

import shapeless._

package object model {
  sealed trait Cardinality
  object Cardinality {
    case object OneN extends Cardinality
    case object NN extends Cardinality
  }

  sealed trait EntityType {
    val isStatic: Boolean
    val entityPath: String
  }
  object EntityType {
    private def s(isStatic: Boolean) = if (isStatic) "static" else "dynamic"

    final case class Node(name: String, isStatic: Boolean = false) extends EntityType {
      override val entityPath: String = s"${s(isStatic)}/${camel(name)}"
    }

    final case class Edge(
      `type`: String,
      source: String,
      destination: String,
      cardinality: Cardinality,
      isStatic: Boolean = false
    ) extends EntityType {
      override val entityPath: String = s"${s(isStatic)}/${camel(source)}_${camel(`type`)}_${camel(destination)}"
    }

    final case class Attr(`type`: String, parent: String, attribute: String, isStatic: Boolean = false) extends EntityType {
      override val entityPath: String = s"${s(isStatic)}/${camel(parent)}_${camel(`type`)}_${camel(attribute)}"
    }
  }

  object EntityTypeLens {
    private val isStaticLens = lens[EntityType].isStatic

    def static(entity: EntityType): Boolean = isStaticLens.get(entity).contains(true)
  }

  case class Graph[D](
    layout: String,
    entities: Map[EntityType, D]
  )

  case class GraphDef(layout: String, entities: Set[EntityType])

  sealed trait BatchPeriod
  object BatchPeriod {
    case object Day extends BatchPeriod
    case object Month extends BatchPeriod
  }
}