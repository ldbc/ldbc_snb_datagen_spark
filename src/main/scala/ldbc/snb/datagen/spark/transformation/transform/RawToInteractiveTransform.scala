package ldbc.snb.datagen.spark.transformation.transform

import ldbc.snb.datagen.spark.transformation.model.{Graph, Mode}
import org.apache.spark.sql.DataFrame

case class RawToInteractiveTransform(bulkLoadThreshold: Long, simulationEnd: Long) extends Transform.Uni[DataFrame] {
  override def transform(input: Graph[DataFrame]): Graph[DataFrame] = {

    val entities = input.entities.map {
      case (tpe, v) if tpe.isStatic => tpe -> v
      case (tpe, v) => tpe -> Interactive.snapshotPart(tpe, v, bulkLoadThreshold, simulationEnd)
    }

    Graph(input.layout, Mode.Interactive, entities)
  }
}

