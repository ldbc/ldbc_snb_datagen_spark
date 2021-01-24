package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.sql._
import ldbc.snb.datagen.transformation.transform.Transform._
import ldbc.snb.datagen.transformation.model.{Batched, BatchedEntity, EntityType, Graph, Mode}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.Logging
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

case class RawToBiTransform(bulkLoadThreshold: Long, simulationEnd: Long) extends Transform.Untyped[Mode.Raw.type, Mode.BI.type] with Logging {
  override def transform(input: DataFrameGraph[Mode.Raw.type]): DataFrameGraph[Mode.BI.type] = {
    val batch_id = (col: Column) =>
      date_format(date_trunc("dd", to_date(col, Raw.dateTimePattern)), "yyyyMMdd")



    val batched = (df: DataFrame) => df
      .select(
        df.columns.map(qcol) ++ Seq(
          batch_id($"creationDate").as("insert_batch_id"),
          batch_id($"deletionDate").as("delete_batch_id")
        ): _*)
      .filter($"insert_batch_id" =!= $"delete_batch_id")

    val insertBatchPart = (tpe: EntityType, df: DataFrame, batchStart: Long, batchEnd: Long) => {
      df
        .filter(Raw.dateTimeToTimestampMillis($"creationDate") >= batchStart && Raw.dateTimeToTimestampMillis($"creationDate") < batchEnd)
        .pipe(batched)
        .select(
          Seq($"insert_batch_id".as("batch_id")) ++ Interactive.columns(tpe, df.columns).map(qcol): _*
        )
    }

    val deleteBatchPart = (tpe: EntityType, df: DataFrame, batchStart: Long, batchEnd: Long) => {
      val idColumns = tpe.primaryKey.map(qcol)
      df
        .filter(Raw.dateTimeToTimestampMillis($"deletionDate") >= batchStart && Raw.dateTimeToTimestampMillis($"deletionDate") < batchEnd)
        .pipe(batched)
        .select(Seq($"delete_batch_id".as("batch_id"), $"deletionDate") ++ idColumns: _*)
    }

    val entities = input.entities.map {
      case (tpe, v) if tpe.isStatic => tpe -> BatchedEntity(v, None, None)
      case (tpe, v) => tpe -> BatchedEntity(
        Interactive.snapshotPart(tpe, v, bulkLoadThreshold, simulationEnd),
        Some(Batched(insertBatchPart(tpe, v, bulkLoadThreshold, simulationEnd), Seq("batch_id"))),
        Some(Batched(deleteBatchPart(tpe, v, bulkLoadThreshold, simulationEnd), Seq("batch_id")))
      )
    }

    Graph[Mode.BI.type, DataFrame]("Bi", entities)
  }
}
