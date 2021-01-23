package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.transformation.model.{Graph, Mode}
import ldbc.snb.datagen.transformation.transform.Transform.DataFrameGraph
import org.apache.spark.sql.DataFrame

case class RawToInteractiveTransform(bulkLoadThreshold: Long, simulationEnd: Long) extends Transform.Untyped[Mode.Raw.type, Mode.Interactive.type] {
  override def transform(input: DataFrameGraph[Mode.Raw.type]): DataFrameGraph[Mode.Interactive.type] = {
    val entities = input.entities.map {
      case (tpe, v) if tpe.isStatic => tpe -> v
      case (tpe, v) => tpe -> Interactive.snapshotPart(tpe, v, bulkLoadThreshold, simulationEnd)
    }
    Graph[Mode.Interactive.type, DataFrame](input.layout, entities)
  }
}

