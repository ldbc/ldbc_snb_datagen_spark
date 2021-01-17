package ldbc.snb.datagen.spark.transformation

import ldbc.snb.datagen.spark.util.Utils.{camel, lower}

package object model {
  sealed trait Cardinality
  object Cardinality {
    case object OneN extends Cardinality
    case object NN extends Cardinality
    case object NOne extends Cardinality
  }

  sealed trait EntityType {
    val isStatic: Boolean
    val entityPath: String
    val primaryKey: Seq[String]
  }
  object EntityType {
    private def s(isStatic: Boolean) = if (isStatic) "static" else "dynamic"

    final case class Node(name: String, isStatic: Boolean = false) extends EntityType {
      override val entityPath: String = s"${s(isStatic)}/${lower(name)}"
      override val primaryKey: Seq[String] = Seq("id")
    }

    final case class Edge(
      `type`: String,
      source: String,
      destination: String,
      cardinality: Cardinality,
      isStatic: Boolean = false
    ) extends EntityType {
      override val entityPath: String = s"${s(isStatic)}/${lower(source)}_${camel(`type`)}_${lower(destination)}"

      override val primaryKey: Seq[String] = ((source, destination) match {
          case (s, d) if s == d => Seq(s"${s}1", s"${d}2")
          case (s, d) => Seq(s, d)
      }).map(name => s"$name.id")
    }

    final case class Attr(`type`: String, parent: String, attribute: String, isStatic: Boolean = false) extends EntityType {
      override val entityPath: String = s"${s(isStatic)}/${lower(parent)}_${camel(`type`)}_${lower(attribute)}"

      override val primaryKey: Seq[String] = ((parent, attribute) match {
        case (s, d) if s == d => Seq(s"${s}1", s"${d}2")
        case (s, d) => Seq(s, d)
      }).map(name => s"$name.id")

    }
  }

  sealed trait Mode
  object Mode {
    final case object Raw extends Mode
    final case object Interactive extends Mode
    final case object BI extends Mode
  }

  case class Graph[D](
    layout: String,
    mode: Mode,
    entities: Map[EntityType, D]
  )

  case class GraphDef(layout: String, mode: Mode, entities: Set[EntityType])

  sealed trait BatchPeriod
  object BatchPeriod {
    case object Day extends BatchPeriod
    case object Month extends BatchPeriod
  }
}