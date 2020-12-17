package ldbc.snb.datagen.spark.transform

import ldbc.snb.datagen.spark.model.Cardinality._
import ldbc.snb.datagen.spark.model.Entity._
import ldbc.snb.datagen.spark.model.Graph
import org.apache.spark.sql.DataFrame

trait Transform {
  def transform(input: Graph): Graph
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
          (ds, edge) => ds.join(edge.dataset, ds("id") == edge.dataset(edge.source), "left_outer")
        }

        name -> n.copy(dataset = mergedDataset)

      case x => x
    }
    Graph(entities)
  }
}

case class RawToBasicTransform(bulkLoadTreshold: Long) extends Transform {
  override def transform(input: Graph): Graph = {
    def withoutDeletionCols(df: DataFrame): DataFrame = df.drop("deletionDate", "explicitlyDeleted")

    input.entities.map {
      case (name, node@Node(ds, isStatic)) if !isStatic =>
        name -> node.copy(dataset = withoutDeletionCols(ds))
      case (name, edge@Edge(ds, isStatic, _, _, _)) if !isStatic =>
        name -> edge.copy(dataset = withoutDeletionCols(ds))
      case (name, attr@Attr(ds, isStatic, _)) if !isStatic =>
        name -> attr.copy(dataset = withoutDeletionCols(ds))
    }
    // filter for bulk load threshold, etc
    ???
  }
}