package ldbc.snb.datagen.transformation

import ldbc.snb.datagen.util.Utils.{camel, lower}

import scala.language.higherKinds

package object model {
  type Id[A] = A
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

  case class Batched[+T](entity: T, batchId: Seq[String])

  case class BatchedEntity[+T](
    snapshot: T,
    insertBatches: Option[Batched[T]],
    deleteBatches: Option[Batched[T]]
  )

  sealed trait Mode {
    type Layout[Data]
    def modePath: String
  }
  object Mode {
    final case object Raw extends Mode {
      type Layout[+Data] = Data
      override val modePath: String = "raw"
    }
    final case class Interactive(bulkLoadPortion: Double) extends Mode {
      type Layout[+Data] = Data
      override val modePath: String = "interactice"
    }
    final case class BI(bulkloadPortion: Double, batchPeriod: String) extends Mode {
      type Layout[+Data] = BatchedEntity[Data]
      override val modePath: String = "bi"
    }
  }

  trait GraphLike[+M <: Mode] {
    def isAttrExploded: Boolean
    def isEdgesExploded: Boolean
    def mode: M
  }

  case class Graph[+M <: Mode, D](
    isAttrExploded: Boolean,
    isEdgesExploded: Boolean,
    mode: M,
    entities: Map[EntityType, M#Layout[D]]
  ) extends GraphLike[M]

  case class GraphDef[M <: Mode](
    isAttrExploded: Boolean,
    isEdgesExploded: Boolean,
    mode: M,
    entities: Set[EntityType]
  ) extends GraphLike[M]

  sealed trait BatchPeriod
  object BatchPeriod {
    case object Day extends BatchPeriod
    case object Month extends BatchPeriod
  }
}