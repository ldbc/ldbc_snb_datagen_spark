package ldbc.snb.datagen.spark.transformation.writer

import better.files._
import ldbc.snb.datagen.spark.transformation.model.{EntityType, Graph}
import ldbc.snb.datagen.spark.transformation.model.EntityType.{Attr, Edge, Node}
import ldbc.snb.datagen.spark.transformation.transform.{Batched, BatchedEntity}
import ldbc.snb.datagen.spark.util.Utils.snake
import org.apache.spark.sql.{DataFrame, DataFrameWriter}
import shapeless._

import scala.collection.immutable.TreeMap


trait CsvWriter[T] {
  def write(graph: Graph[T], outputDir: String): Unit
}

object CsvWriter {
  object implicits {
    implicit val cacheFriendlyOrdering = new Ordering[EntityType] {
      private val and = (a: Int, b: Int) => if (a == 0) b else a

      private object mapper extends Poly1 {
        implicit val atNode = at[Node](n => Seq(n.name.hashCode, 0, n.hashCode()))
        implicit val atEdge = at[Edge](e => {
          val primary = e match {
            case Edge("Likes", "Person", "Comment", _, _) => "Comment"
            case Edge("Likes", "Person", "Post", _, _) => "Post"
            case Edge("ContainerOf", "Forum", "Post", _, _) => "Post"
            case Edge(_, source, _, _, _) => source
          }
          Seq(primary.hashCode, 2, e.hashCode)
        })
        implicit val atAttr = at[Attr](a => Seq(a.parent.hashCode, 1, a.hashCode()))
      }

      private def orderingKeys(t: EntityType): Seq[Int] = Generic[EntityType].to(t).map(mapper).unify

      override def compare(x: EntityType, y: EntityType): Int =
        orderingKeys(x).zip(orderingKeys(y)).foldLeft(0)((a, v) => and(a, Ordering[Int].compare(v._1, v._2)))
    }
  }

  def commonCsvOptions[T](dfw: DataFrameWriter[T], header: Boolean, separator: Char) = dfw
    .format("csv")
    .options(Map(
      "header" -> header.toString,
      "sep" -> separator.toString
    ))
}

case class GraphCsvWriter(header: Boolean = false, separator: Char = '|') extends CsvWriter[DataFrame] {
  import CsvWriter.implicits._


  def write(graph: Graph[DataFrame], outputDir: String) = {
    TreeMap(graph.entities.toSeq: _*).foreach {
      case (tpe, dataset) =>
        println(tpe)
        CsvWriter.commonCsvOptions(dataset.write, header, separator)
          .save((outputDir / "csv" / snake(graph.layout) / tpe.entityPath).toString())
    }
  }
}

case class BatchedGraphCsvWriter(header: Boolean = false, separator: Char = '|') extends CsvWriter[BatchedEntity[DataFrame]] {
  import CsvWriter.implicits._

  override def write(graph: Graph[BatchedEntity[DataFrame]], outputDir: String): Unit = {
    TreeMap(graph.entities.toSeq: _*).foreach {
      case (tpe, BatchedEntity(snapshot, insertBatches, deleteBatches)) =>
        println(s"Writing batches for $tpe")
        CsvWriter.commonCsvOptions(snapshot.write, header, separator)
          .save((outputDir / "csv" / snake(graph.layout) / "initial_snapshot" / tpe.entityPath).toString())

        insertBatches.foreach { case Batched(entity, partitionKeys) =>
          CsvWriter.commonCsvOptions(entity.write, header, separator)
            .partitionBy(partitionKeys: _*)
            .save((outputDir / "csv" / snake(graph.layout) / "inserts" / tpe.entityPath).toString())
        }

        deleteBatches.foreach { case Batched(entity, partitionKeys) =>
          CsvWriter.commonCsvOptions(entity.write, header, separator)
            .partitionBy(partitionKeys: _*)
            .save((outputDir / "csv" / snake(graph.layout) / "deletes" / tpe.entityPath).toString())
        }
    }

  }
}