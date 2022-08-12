package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.model.{Batched, BatchedEntity, EntityType, Mode}
import ldbc.snb.datagen.util.sql.qcol
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DateType, TimestampType}
import shapeless._

trait ConvertDates[M <: Mode] extends Transform[M, M] {
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
}

object ConvertDates {
  def apply[T <: Mode : ConvertDates] = implicitly[ConvertDates[T]]

  object instances {
    implicit def batchedConvertDates[M <: Mode](implicit ev: BatchedEntity =:= M#Layout) = new ConvertDates[M] {
      override def transform(input: In): Out = {
        lens[In].entities.modify(input)(
          _.map { case (tpe, layout) => tpe -> {
            val be = layout.asInstanceOf[BatchedEntity]
            ev(BatchedEntity(
              convertDates(tpe, be.snapshot),
              be.insertBatches.map(b => Batched(convertDates(tpe, b.entity), b.batchId, b.ordering)),
              be.deleteBatches.map(b => Batched(convertDates(tpe, b.entity), b.batchId, b.ordering))
            ))
          }
          }
        )
      }
    }

    implicit def simpleConvertDates[M <: Mode](implicit ev: DataFrame =:= M#Layout) = new ConvertDates[M] {
      override def transform(input: In): Out = {
        lens[In].entities.modify(input)(
          _.map { case (tpe, v) => tpe -> ev(convertDates(tpe, v.asInstanceOf[DataFrame])) }
        )
      }
    }
  }
}
