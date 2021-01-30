package ldbc.snb.datagen.transformation.io

import ldbc.snb.datagen.transformation.model.{Batched, BatchedEntity, EntityType, Graph, Mode}
import ldbc.snb.datagen.transformation.model.EntityType.{Attr, Edge, Node}
import ldbc.snb.datagen.util.Utils.snake
import org.apache.spark.sql.{DataFrame, DataFrameWriter}
import shapeless.{Generic, Poly1}
import better.files._
import ldbc.snb.datagen.util.Logging
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.TreeMap

case class WriterOptions(
  header: Boolean = true,
  separator: Char = '|'
)

trait GraphWriter[M <: Mode] {
  type Data
  def write(graph: Graph[M, Data], path: String, options: WriterOptions): Unit
}

object GraphWriter {
  type Aux[M <: Mode, D] = GraphWriter[M] { type Data = D }
  def apply[M <: Mode, D](implicit ev: GraphWriter.Aux[M, D]): GraphWriter.Aux[M, D] = ev
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

private final class DataFrameGraphWriter[M <: Mode](implicit ev: M#Layout[DataFrame] =:= DataFrame) extends GraphWriter[M] with Logging {
  import CsvWriter.implicits._
  type Data = DataFrame

  override def write(graph: Graph[M, DataFrame], path: String, options: WriterOptions): Unit = {
    TreeMap(graph.entities.toSeq: _*).foreach {
      case (tpe, dataset) =>
        log.info(f"$tpe: Writing snapshot")

        CsvWriter.commonCsvOptions(dataset.write, options.header, options.separator)
          .save((path / "csv" / snake(graph.layout) / tpe.entityPath).toString())
    }
  }
}

private final class BatchedDataFrameGraphWriter[M <: Mode](implicit ev: M#Layout[DataFrame] =:= BatchedEntity[DataFrame]) extends GraphWriter[M] with Logging {
  import CsvWriter.implicits._
  type Data = DataFrame

  override def write(graph: Graph[M, DataFrame], path: String, options: WriterOptions): Unit = {
    TreeMap(graph.entities.mapValues(ev).toSeq: _*).foreach {
      case (tpe, BatchedEntity(snapshot, insertBatches, deleteBatches)) =>

        log.info(f"$tpe: Writing initial snapshot")

        CsvWriter.commonCsvOptions(snapshot.write, options.header, options.separator)
          .save((path / "csv" / snake(graph.layout) / "initial_snapshot" / tpe.entityPath).toString())

        log.info(f"$tpe: Writing inserts")

        insertBatches.foreach { case Batched(entity, partitionKeys) =>
          CsvWriter.commonCsvOptions(entity.write, options.header, options.separator)
            .partitionBy(partitionKeys: _*)
            .save((path / "csv" / snake(graph.layout) / "inserts" / tpe.entityPath).toString())
        }

        log.info(f"$tpe: Writing deletes")

        deleteBatches.foreach { case Batched(entity, partitionKeys) =>
          CsvWriter.commonCsvOptions(entity.write, options.header, options.separator)
            .partitionBy(partitionKeys: _*)
            .save((path / "csv" / snake(graph.layout) / "deletes" / tpe.entityPath).toString())
        }
    }
  }
}

trait GraphWriterInstances {
  implicit def dataFrameGraphWriter[M <: Mode]
  (implicit ev: M#Layout[DataFrame] =:= DataFrame): GraphWriter.Aux[M, DataFrame] = new DataFrameGraphWriter[M]
  implicit def batchedDataFrameGraphWriter[M <: Mode]
  (implicit ev: M#Layout[DataFrame] =:= BatchedEntity[DataFrame]): GraphWriter.Aux[M, DataFrame] = new BatchedDataFrameGraphWriter[M]
}
