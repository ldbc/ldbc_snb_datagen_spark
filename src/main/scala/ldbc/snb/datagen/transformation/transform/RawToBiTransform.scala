package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.model.Cardinality._
import ldbc.snb.datagen.model.EntityType._
import ldbc.snb.datagen.model.Mode.BI
import ldbc.snb.datagen.model._
import ldbc.snb.datagen.model.raw.PersonType
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.Logging
import ldbc.snb.datagen.util.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

case class RawToBiTransform(mode: BI, simulationStart: Long, simulationEnd: Long, keepImplicitDeletes: Boolean)
    extends Transform[Mode.Raw.type, Mode.BI]
    with Logging {
  log.debug(s"BI Transformation parameters: $mode")

  val bulkLoadThreshold = calculateBulkLoadThreshold(mode.bulkloadPortion, simulationStart, simulationEnd)

  def calculateBulkLoadThreshold(bulkLoadPortion: Double, simulationStart: Long, simulationEnd: Long) = {
    (simulationEnd - ((simulationEnd - simulationStart) * (1 - bulkLoadPortion)).toLong)
  }

  def batchPeriodFormat(batchPeriod: String) = batchPeriod match {
    case "year"   => "yyyy"
    case "month"  => "yyyy-MM"
    case "day"    => "yyyy-MM-dd"
    case "hour"   => "yyyy-MM-dd'T'hh"
    case "minute" => "yyyy-MM-dd'T'hh:mm"
    case _        => throw new IllegalArgumentException("Unrecognized partition key")
  }

  private def notDerived(entityType: EntityType): Boolean = entityType match {
    case Edge(_, _, _, OneN, _, _, _) => false
    case Edge(_, _, _, NOne, _, _, _) => false
    case Attr(_, _, _, _) => false
    case _ => true
  }

  def columns(tpe: EntityType, cols: Seq[String]) = tpe match {
    case tpe if tpe.isStatic => cols
    case Edge("Knows", PersonType, PersonType, NN, false, _, _) =>
      val rawCols = Set("deletionDate", "explicitlyDeleted", "weight")
      cols.filter(!rawCols.contains(_))
    case _ =>
      val rawCols = Set("deletionDate", "explicitlyDeleted")
      cols.filter(!rawCols.contains(_))
  }

  def snapshotPart(tpe: EntityType, df: DataFrame, bulkLoadThreshold: Long, filterDeletion: Boolean) = {
    val filterBulkLoad = (ds: DataFrame) =>
      ds
        .filter(
          $"creationDate" < lit(bulkLoadThreshold) &&
            (!lit(filterDeletion) || $"deletionDate" >= lit(bulkLoadThreshold))
        )

    tpe match {
      case tpe if tpe.isStatic => df
      case tpe                 => filterBulkLoad(df).select(columns(tpe, df.columns).map(name => col(qualified(name))): _*)
    }
  }

  override def transform(input: In): Out = {
    val batch_id = (col: Column) => date_format(date_trunc(mode.batchPeriod, to_timestamp(col / lit(1000L))), batchPeriodFormat(mode.batchPeriod))

    def inBatch(col: Column, batchStart: Long, batchEnd: Long) =
      col >= lit(batchStart) && col < lit(batchEnd)

    val batched = (df: DataFrame) =>
      df
        .select(
          df.columns.map(qcol) ++ Seq(
            batch_id($"creationDate").as("insert_batch_id"),
            batch_id($"deletionDate").as("delete_batch_id")
          ): _*
        )

    val insertBatchPart = (tpe: EntityType, df: DataFrame, batchStart: Long, batchEnd: Long) => {
      df
        .filter(inBatch($"creationDate", batchStart, batchEnd))
        .pipe(batched)
        .select(
          Seq($"insert_batch_id".as("batch_id")) ++ columns(tpe, df.columns).map(qcol): _*
        )
    }

    val deleteBatchPart = (tpe: EntityType, df: DataFrame, batchStart: Long, batchEnd: Long) => {
      val idColumns = tpe.primaryKey.map(qcol)
      df
        .filter(inBatch($"deletionDate", batchStart, batchEnd))
        .filter(if (df.columns.contains("explicitlyDeleted")) col("explicitlyDeleted") else lit(true))
        .pipe(batched)
        .select(Seq($"delete_batch_id".as("batch_id"), $"deletionDate") ++ idColumns: _*)
    }

    val entities = input.entities
      .map {
        case (tpe, v) if tpe.isStatic => tpe -> BatchedEntity(v, None, None)
        case (tpe, v) =>
          tpe -> BatchedEntity(
            snapshotPart(tpe, v, bulkLoadThreshold, filterDeletion = false),
            Some(Batched(insertBatchPart(tpe, v, bulkLoadThreshold, simulationEnd), Seq("batch_id"), Seq($"creationDate"))),
            if (notDerived(tpe) && (keepImplicitDeletes || v.columns.contains("explicitlyDeleted")))
              Some(Batched(deleteBatchPart(tpe, v, bulkLoadThreshold, simulationEnd), Seq("batch_id"), Seq($"deletionDate")))
            else
              None
          )
      }

    Graph[Mode.BI](
      GraphDef[Mode.BI](
        isAttrExploded = input.definition.isAttrExploded,
        isEdgesExploded = input.definition.isEdgesExploded,
        useTimestamp = input.definition.useTimestamp,
        mode = mode,
        entities = input.definition.entities
      ),
      entities
    )
  }
}
