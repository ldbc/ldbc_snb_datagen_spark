package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.model.{Batched, BatchedEntity, EntityType, Graph, Mode}
import ldbc.snb.datagen.util.sql.qcol
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DateType, TimestampType}
import shapeless._

trait ConvertDates[M <: Mode] extends Transform[M, M] {
  def convertDatesInEntities(entities: Map[EntityType, M#Layout]): Map[EntityType, (Option[String], M#Layout)]

  def convertDates(tpe: EntityType, df: DataFrame): DataFrame = {
    tpe match {
      case tpe if !tpe.isStatic =>
        df.select(df.columns.map {
          case col@("creationDate" | "deletionDate") => (qcol(col) / lit(1000L)).cast(TimestampType).as(col)
          case col@"birthday" => (qcol(col) / lit(1000L)).cast(TimestampType).cast(DateType).as(col)
          case col => qcol(col)
        }: _*)
      case _ => df
    }
  }

  override def transform(input: Graph[M]): Graph[M] = {
    if (input.definition.useTimestamp) {
      throw new AssertionError("Already using timestamp for dates")
    }

    val updatedEntities = convertDatesInEntities(input.entities)

    val modifiedEntities = updatedEntities
      .map { case (k, (_, data)) => k -> data }

    val modifiedEntityDefinitions = updatedEntities
      .map { case (k, (schema, _)) => k -> schema }

    val l = lens[In]
    (l.definition.useTimestamp ~ l.definition.entities ~ l.entities).set(input)((true, modifiedEntityDefinitions, modifiedEntities))
  }
}

object ConvertDates {
  def apply[T <: Mode : ConvertDates] = implicitly[ConvertDates[T]]

  object instances {
    implicit def batchedConvertDates[M <: Mode](implicit ev: BatchedEntity =:= M#Layout) = new ConvertDates[M] {
      override def convertDatesInEntities(entities: Map[EntityType,M#Layout]): Map[EntityType, (Option[String], M#Layout)] = {
        entities.map { case (tpe, layout) =>
          val be = layout.asInstanceOf[BatchedEntity]
          val convertedSnapshot = convertDates(tpe, be.snapshot)

          val convertedInserts = be.insertBatches.map(b => Batched(convertDates(tpe, b.entity), b.batchId, b.ordering))
          val convertedDeletes = be.deleteBatches.map(b => Batched(convertDates(tpe, b.entity), b.batchId, b.ordering))
          (tpe, (
            Some(convertedSnapshot.schema.toDDL),
            ev(BatchedEntity(convertedSnapshot, convertedInserts, convertedDeletes))
          ))
        }
      }
    }

    implicit def simpleConvertDates[M <: Mode](implicit ev: DataFrame =:= M#Layout) = new ConvertDates[M] {
      override def convertDatesInEntities(entities: Map[EntityType, M#Layout]): Map[EntityType, (Option[String], M#Layout)] = {
        entities.map { case (tpe, v) =>
          val convertedSnapshot = convertDates(tpe, v.asInstanceOf[DataFrame])
          (tpe, (Some(convertedSnapshot.schema.toDDL), ev(convertedSnapshot)))
        }
      }
    }
  }
}
