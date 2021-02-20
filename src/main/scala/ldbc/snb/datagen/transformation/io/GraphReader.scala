package ldbc.snb.datagen.transformation.io

import ldbc.snb.datagen.transformation.model.{Graph, GraphDef, GraphLike, Id, Mode}
import org.apache.spark.sql.{DataFrame, SparkSession}
import better.files._
import ldbc.snb.datagen.transformation.model.Mode.Raw
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

trait GraphReader[M <: Mode] {
  type Data
  def read(graphDef: GraphDef[M], path: String, options: FormatOptions): Graph[M, Data]
  def exists(graphDef: GraphDef[M], path: String): Boolean
}

object GraphReader {
  type Aux[M <: Mode, D] = GraphReader[M] { type Data = D }

  def apply[M <: Mode, D](implicit ev: GraphReader.Aux[M, D]): GraphReader.Aux[M, D] = ev
}

object Reader {
  val defaultCsvOptions = Map(
    "header" -> "true",
    "sep" -> "|"
  )

  def apply(readerOptions: FormatOptions)(implicit spark: SparkSession) = {
    val formatOptions = readerOptions.format match {
      case "csv" => defaultCsvOptions ++ readerOptions.formatOptions
      case _ => readerOptions.formatOptions
    }
    spark.read.format(readerOptions.format).options(formatOptions)
  }
}

private final class DataFrameGraphReader[M <: Mode](implicit spark: SparkSession, ev: Id[DataFrame] =:= M#Layout[DataFrame]) extends GraphReader[M] {
  type Data = DataFrame

  override def read(definition: GraphDef[M], path: String, options: FormatOptions): Graph[M, DataFrame] = {
    val entities = (for { entity <- definition.entities } yield {
      val df = Reader(options).load((path / options.format / PathComponent[GraphLike[M]].path(definition) / entity.entityPath).toString())
      entity -> ev(df)
    }).toMap
    Graph[M, DataFrame](definition.isAttrExploded, definition.isEdgesExploded, definition.mode, entities)
  }

  override def exists(graphDef: GraphDef[M], path: String): Boolean = {
    val hadoopPath = new Path(path)
    FileSystem.get(new URI(path), spark.sparkContext.hadoopConfiguration).exists(hadoopPath)
  }
}

trait GraphReaderInstances {
  implicit def dataFrameGraphReader[M <: Mode]
  (implicit spark: SparkSession, ev: Id[DataFrame] =:= M#Layout[DataFrame]): GraphReader.Aux[M, DataFrame] =
    new DataFrameGraphReader[M]
}
