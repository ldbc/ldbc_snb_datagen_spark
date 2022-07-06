package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.model.Cardinality._
import ldbc.snb.datagen.model.EntityType._
import ldbc.snb.datagen.model.raw._
import ldbc.snb.datagen.model.{EntityType, Graph, Mode}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.Logging
import ldbc.snb.datagen.util.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

case class RawToInteractiveTransform(mode: Mode.Interactive, simulationStart: Long, simulationEnd: Long)
    extends Transform[Mode.Raw.type, Mode.Interactive]
    with Logging {

  val bulkLoadThreshold = RawToInteractiveTransform.calculateBulkLoadThreshold(mode.bulkloadPortion, simulationStart, simulationEnd)

  override def transform(input: In): Out = {
    log.info(s"Running $this")
    val entities = input.entities
      .map { case (tpe, v) =>
        tpe -> RawToInteractiveTransform.snapshotPart(tpe, v, bulkLoadThreshold, filterDeletion = true)
      }
    Graph[Mode.Interactive](isAttrExploded = input.isAttrExploded, isEdgesExploded = input.isEdgesExploded, mode, entities)
  }
}

object RawToInteractiveTransform {

  def columns(tpe: EntityType, cols: Seq[String]) = tpe match {
    case tpe if tpe.isStatic => cols
    case Edge("Knows", PersonType, PersonType, NN, false, _, _) =>
      val rawCols = Set("deletionDate", "explicitlyDeleted", "weight")
      cols.filter(!rawCols.contains(_))
    case _ =>
      val rawCols = Set("deletionDate", "explicitlyDeleted")
      cols.filter(!rawCols.contains(_))
  }

  def calculateBulkLoadThreshold(bulkLoadPortion: Double, simulationStart: Long, simulationEnd: Long) = {
    (simulationEnd - ((simulationEnd - simulationStart) * (1 - bulkLoadPortion)).toLong)
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
}
