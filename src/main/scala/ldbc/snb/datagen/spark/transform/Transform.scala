package ldbc.snb.datagen.spark.transform

import ldbc.snb.datagen.model.Cardinality._
import ldbc.snb.datagen.model.Entity._
import ldbc.snb.datagen.model.{Entity, EntityLens, Graph}
import org.apache.spark.sql.DataFrame

trait Transform {
  def transform(input: Graph): Graph
}

case class RawToBasicTransform(bulkLoadTreshold: Long, simulationEnd: Long) extends Transform {
  override def transform(input: Graph): Graph = {

    val withoutDeletionCols = (ds: DataFrame) => ds
      .drop("deletionDate", "explicitlyDeleted")

    val filterBulkLoad = (ds: DataFrame) => ds
      .filter(ds("creationDate") < bulkLoadTreshold &&
        ds("deletionDate") >= bulkLoadTreshold &&
        ds("deletionDate") <= simulationEnd
      )

    val entities = input.entities.map {
      case (name, entity) if !EntityLens.static(entity) =>
        name -> EntityLens.transformed(entity, filterBulkLoad andThen withoutDeletionCols)
      case x => x
    }

    Graph(entities)
  }
}

object BasicToMergeForeignTransform extends Transform {
  override def transform(input: Graph) = {
    val (edgesToMerge, rest) = input.entities.partition {
      case (_, Edge(_, _, _, _, cardinality)) if cardinality == N1 => true
      case _ => false
    }

    val entities = rest.map {
      case (name, n@Node(dataset, _)) =>
        val mergeTargets = edgesToMerge.collect {
          case (_, edge@Edge(_, _, source, _, _)) if source == name => edge
        }

        val mergedDataset = mergeTargets.foldLeft(dataset) {
          (ds, edge) => ds.join(edge.dataset, ds("id") === edge.dataset(edge.source), "left_outer")
        }

        name -> n.copy(dataset = mergedDataset)

      case x => x
    }
    Graph(entities)
  }
}