package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.transformation.model.EntityType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.unix_timestamp

object Raw {
  def withRawColumns(et: EntityType, cols: Column*): Seq[Column] = (!et.isStatic).fork.foldLeft(cols)((cols, _) => Seq(
    $"creationDate".as("creationDate"),
    $"deletionDate".as("deletionDate")
  ) ++ cols)

  def dateTimePattern = "yyyy-MM-dd'T'HH:mm:ss.SSS+00:00"

  def dateTimeToTimestampMillis(col: Column) = unix_timestamp(col, Raw.dateTimePattern) * 1000

}
