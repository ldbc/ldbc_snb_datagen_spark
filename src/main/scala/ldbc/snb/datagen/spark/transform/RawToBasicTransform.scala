package ldbc.snb.datagen.spark.transform

import ldbc.snb.datagen.model.{EntityTypeLens, Graph}
import ldbc.snb.datagen.spark.model.DataFrameGraph
import org.apache.spark.sql.DataFrame

case class RawToBasicTransform(bulkLoadTreshold: Long, simulationEnd: Long) extends Transform {
  override def transform(input: DataFrameGraph): DataFrameGraph = {

    val withoutDeletionCols = (ds: DataFrame) => ds
      .drop("deletionDate", "explicitlyDeleted")

    val filterBulkLoad = (ds: DataFrame) => ds
      .filter(ds("creationDate") < bulkLoadTreshold &&
        ds("deletionDate") >= bulkLoadTreshold &&
        ds("deletionDate") <= simulationEnd
      )

    val entities = input.entities.map {
      case (tpe, entity) if !EntityTypeLens.static(tpe) =>
        tpe -> (filterBulkLoad andThen withoutDeletionCols)(entity)
      case x => x
    }

    Graph("Basic", entities)
  }
}