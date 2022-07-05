package ldbc.snb.datagen

import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.pascalToCamel
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Encoder}
import shapeless._

import scala.language.higherKinds

package object model {
  type Id[A] = A
  sealed trait Cardinality
  object Cardinality {
    case object OneN extends Cardinality
    case object NN   extends Cardinality
    case object NOne extends Cardinality
  }

  trait EntityTraits[A] {
    def `type`: EntityType
    def schema: StructType
    def sizeFactor: Double
  }

  object EntityTraits {
    def apply[A: EntityTraits] = implicitly[EntityTraits[A]]

    def pure[A: Encoder](`type`: EntityType, sizeFactor: Double): EntityTraits[A] = {
      val _type       = `type`
      val _sizeFactor = sizeFactor
      val _schema     = implicitly[Encoder[A]].schema
      new EntityTraits[A] {
        override def `type`: EntityType = _type
        override def schema: StructType = _schema
        override def sizeFactor: Double = _sizeFactor
      }
    }
  }

  sealed trait EntityType {
    val isStatic: Boolean
    val entityPath: String
    val primaryKey: Seq[String]
  }

  object EntityType {
    private def s(isStatic: Boolean) = if (isStatic) "static" else "dynamic"

    final case class Node(name: String, isStatic: Boolean = false) extends EntityType {
      override val entityPath: String      = s"${s(isStatic)}/$name"
      override val primaryKey: Seq[String] = Seq("id")
      override def toString: String        = s"$name"
    }

    final case class Edge(
        `type`: String,
        source: Node,
        destination: Node,
        cardinality: Cardinality,
        isStatic: Boolean = false,
        sourceNameOverride: Option[String] = None,
        destinationNameOverride: Option[String] = None
    ) extends EntityType {
      val sourceName: String = sourceNameOverride.getOrElse(source.name)
      val destinationName: String = destinationNameOverride.getOrElse(destination.name)

      override val entityPath: String = s"${s(isStatic)}/${sourceName}_${pascalToCamel(`type`)}_${destinationName}"

      override val primaryKey: Seq[String] = ((sourceName, destinationName) match {
        case (s, d) if s == d => Seq(s"${s}1", s"${d}2")
        case (s, d)           => Seq(s, d)
      }).map(name => s"${name}Id")

      override def toString: String = s"${sourceName} -[${`type`}]-> ${destinationName}"
    }

    final case class Attr(`type`: String, parent: Node, attribute: String, isStatic: Boolean = false) extends EntityType {
      override val entityPath: String = s"${s(isStatic)}/${parent.name}_${pascalToCamel(`type`)}_${attribute}"

      override val primaryKey: Seq[String] = ((parent.name, attribute) match {
        case (s, d) if s == d => Seq(s"${s}1", s"${d}2")
        case (s, d)           => Seq(s, d)
      }).map(name => s"${name}Id")
      override def toString: String = s"${parent.name} ♢-[${`type`}]-> $attribute"
    }

  }

  // Gadget to move from a strongly typed entity to an EntityType -> Schema mapping
  trait UntypedEntities[T] { def value: Map[EntityType, StructType] }

  object UntypedEntities {
    implicit def apply[T: UntypedEntities] = implicitly[UntypedEntities[T]]
  }

  trait UntypedEntitiesInstances {

    implicit def untypedEntitiesForHCons[T, Rest <: Coproduct](implicit et: EntityTraits[T], ev: UntypedEntities[Rest]): UntypedEntities[T :+: Rest] =
      new UntypedEntities[T :+: Rest] {
        override val value: Map[EntityType, StructType] = ev.value + (et.`type` -> et.schema)
      }

    implicit val untypedEntitiesForHNil: UntypedEntities[CNil] = new UntypedEntities[CNil] {
      override def value: Map[EntityType, StructType] = Map.empty
    }

    implicit def untypedEntitiesForEnum[T, G](implicit gen: Generic.Aux[T, G], ev: UntypedEntities[G]): UntypedEntities[T] = new UntypedEntities[T] {
      override val value = ev.value
    }
  }

  case class Batched(entity: DataFrame, batchId: Seq[String], ordering: Seq[Column])

  case class BatchedEntity(
      snapshot: DataFrame,
      insertBatches: Option[Batched],
      deleteBatches: Option[Batched]
  )

  sealed trait Mode {
    type Layout
    def modePath: String
  }
  object Mode {
    final case object Raw extends Mode {
      type Layout = DataFrame
      override val modePath: String = "raw"

      def withRawColumns(et: EntityType, cols: Column*): Seq[Column] = (!et.isStatic).fork.foldLeft(cols)((cols, _) =>
        Seq(
          $"creationDate".as("creationDate"),
          $"deletionDate".as("deletionDate"),
          $"explicitlyDeleted".as("explicitlyDeleted")
        ) ++ cols
      )

      def dateTimePattern = "yyyy-MM-dd'T'HH:mm:ss.SSS+00:00"
      def datePattern     = "yyyy-MM-dd"

    }
    final case class Interactive(bulkLoadPortion: Double) extends Mode {
      type Layout = DataFrame
      override val modePath: String = "interactive"
    }
    final case class BI(bulkloadPortion: Double, batchPeriod: String) extends Mode {
      type Layout = BatchedEntity
      override val modePath: String = "bi"
    }
  }

  trait GraphLike[+M <: Mode] {
    def isAttrExploded: Boolean
    def isEdgesExploded: Boolean
    def mode: M
  }

  case class Graph[+M <: Mode](
      isAttrExploded: Boolean,
      isEdgesExploded: Boolean,
      mode: M,
      entities: Map[EntityType, M#Layout]
  ) extends GraphLike[M]

  case class GraphDef[M <: Mode](
      isAttrExploded: Boolean,
      isEdgesExploded: Boolean,
      mode: M,
      entities: Map[EntityType, Option[String]]
  ) extends GraphLike[M]

  sealed trait BatchPeriod
  object BatchPeriod {
    case object Day   extends BatchPeriod
    case object Month extends BatchPeriod
  }
  object instances extends UntypedEntitiesInstances
}
