package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.model.{EntityType, Mode}
import ldbc.snb.datagen.util.sql.qcol
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DateType, TimestampType}
import shapeless._

object ConvertDates extends Transform[Mode.Raw.type, Mode.Raw.type] {

  def convertDates(tpe: EntityType, df: DataFrame): DataFrame = {
    tpe match {
      case tpe if !tpe.isStatic =>
        df.select(df.columns.map {
          case col @ ("creationDate" | "deletionDate") => (qcol(col) / lit(1000L)).cast(TimestampType).as(col)
          case col @ "birthday"                        => (qcol(col) / lit(1000L)).cast(TimestampType).cast(DateType).as(col)
          case col                                     => qcol(col)
        }: _*)
      case _ => df
    }
  }

  override def transform(input: In): Out = {
    lens[In].entities.modify(input)(
      _.map { case (tpe, v) => tpe -> convertDates(tpe, v) }
    )
  }
}
