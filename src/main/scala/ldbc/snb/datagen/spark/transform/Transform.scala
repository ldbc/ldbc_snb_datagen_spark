package ldbc.snb.datagen.spark.transform

import ldbc.snb.datagen.model.Cardinality._
import ldbc.snb.datagen.model.EntityType._
import ldbc.snb.datagen.model.{EntityTypeLens, Graph}
import ldbc.snb.datagen.spark.model.DataFrameGraph
import org.apache.spark.sql.DataFrame

trait Transform {
  def transform(input: DataFrameGraph): DataFrameGraph
}

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

object BasicToMergeForeignTransform extends Transform {
  override def transform(input: DataFrameGraph): DataFrameGraph = {
    val (edgesToMerge, rest) = input.entities.partition {
      case (Edge(_, _, _, cardinality, _), _) if cardinality == N1 => true
      case _ => false
    }

    val entities = rest.map {
      case (n@Node(name, _), dataset) =>
        val mergeTargets = edgesToMerge.collect {
          case (edge@Edge(_, source, _, _, _), dataset) if source == name => (dataset, edge.source)
        }

        val mergedDataset = mergeTargets.foldLeft(dataset) {
          (ds, edge) => ds.join(edge._1, ds("id") === edge._1(edge._2), "left_outer")
        }

        n -> mergedDataset

      case x => x
    }
    Graph("MergeForeign", entities)
  }
}