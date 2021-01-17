package ldbc.snb.datagen.spark.transformation.transform

import ldbc.snb.datagen.spark.sql._
import ldbc.snb.datagen.spark.transformation.model
import ldbc.snb.datagen.spark.transformation.model.{EntityType, Graph, Mode}
import ldbc.snb.datagen.syntax.FluentSyntaxForAny
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{date_trunc, _}
import org.apache.spark.sql.types.DateType

case class Batched[T](
  entity: T,
  partitionKeys: Seq[String]
)

case class BatchedEntity[T](
  snapshot: T,
  insertBatches: Option[Batched[T]],
  deleteBatches: Option[Batched[T]]
)

case class RawToBiTransform(bulkLoadTreshold: Long, simulationEnd: Long) extends Transform[DataFrame, BatchedEntity[DataFrame]] {
  def transform(input: model.Graph[DataFrame]): model.Graph[BatchedEntity[DataFrame]] = {

    val batched = (df: DataFrame) => df
      .select(
        df.columns.map(qcol) ++ Seq(
          date_trunc("yyyy-MM-dd", $"creationDate".cast(DateType)).as("insert_batch_id"),
          date_trunc("yyyy-MM-dd", $"deletionDate".cast(DateType)).as("delete_batch_id")
        ): _*)
      .filter($"insert_batch_id" =!= $"delete_batch_id")

    val insertBatchPart = (tpe: EntityType, df: DataFrame, batchStart: Long, batchEnd: Long) => {
      df
        .filter($"creationDate" >= batchStart && $"creationDate" < batchEnd)
        .let(batched)
        .select(
          Seq($"insert_batch_id".as("batch_id")) ++
            Interactive.columns(tpe, df.columns).map(qcol): _*
        )
    }

    val deleteBatchPart = (tpe: EntityType, df: DataFrame, batchStart: Long, batchEnd: Long) => {
      val idColumns = tpe.primaryKey.map(qcol)
      df
        .filter($"deletionDate" >= batchStart && $"deletionDate" < batchEnd)
        .let(batched)
        .select(Seq($"delete_batch_id".as("batch_id"), $"deletionDate") ++ idColumns: _*)
    }


    val entities = input.entities.map {
      case (tpe, v) if tpe.isStatic => tpe -> BatchedEntity(v, None, None)
      case (tpe, v) => tpe -> BatchedEntity(
        Interactive.snapshotPart(tpe, v, bulkLoadTreshold, simulationEnd),
        Some(Batched(insertBatchPart(tpe, v, bulkLoadTreshold, simulationEnd), Seq("batch_id"))),
        Some(Batched(deleteBatchPart(tpe, v, bulkLoadTreshold, simulationEnd), Seq("batch_id")))
      )
    }

    Graph("Bi", Mode.BI, entities)
  }
}
