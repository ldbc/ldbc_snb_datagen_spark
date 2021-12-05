package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.model.{Graph, Mode}
import ldbc.snb.datagen.util.Logging
import org.apache.spark.sql.DataFrame

case class RawToInteractiveTransform(mode: Mode.Interactive, simulationStart: Long, simulationEnd: Long)
    extends Transform[Mode.Raw.type, Mode.Interactive]
    with Logging {
  log.debug(s"Interactive Transformation parameters: $mode")

  val bulkLoadThreshold = Interactive.calculateBulkLoadThreshold(mode.bulkLoadPortion, simulationStart, simulationEnd)

  override def transform(input: In): Out = {
    val entities = input.entities.map {
      case (tpe, v) if tpe.isStatic => tpe -> v
      case (tpe, v)                 => tpe -> Interactive.snapshotPart(tpe, v, bulkLoadThreshold, filterDeletion = true)
    }
    Graph[Mode.Interactive](isAttrExploded = input.isAttrExploded, isEdgesExploded = input.isEdgesExploded, mode, entities)
  }
}
