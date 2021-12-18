package ldbc.snb.datagen.io

import ldbc.snb.datagen.syntax._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object dataframes {

  case class DataFrameSource(
      path: String,
      format: String,
      formatOptions: Map[String, String] = Map.empty,
      schema: Option[StructType] = None
  )

  private class DataFrameReader(implicit spark: SparkSession) extends Reader[DataFrameSource] {
    override type Ret = DataFrame

    override def read(self: DataFrameSource): DataFrame = {
      spark.read
        .format(self.format)
        .options(self.formatOptions)
        .pipeFoldLeft(self.schema)(_ schema _)
        .load(self.path)
    }

    override def exists(self: DataFrameSource): Boolean = utils.fileExists(self.path)
  }

  trait ReaderInstances {
    implicit def dataFrameReader(implicit spark: SparkSession): Reader.Aux[DataFrameSource, DataFrame] = new DataFrameReader
  }

  case class DataFrameSink(
      path: String,
      format: String,
      formatOptions: Map[String, String] = Map.empty,
      mode: SaveMode = SaveMode.ErrorIfExists,
      partitionBy: Seq[String] = Seq.empty
  )

  case class DataFrameWriterOptions(
      format: String,
      partitionBy: Seq[String] = Seq.empty,
      formatOptions: Map[String, String] = Map.empty,
      mode: SaveMode = SaveMode.ErrorIfExists
  )

  private object DataFrameWriter extends Writer[DataFrameSink] {
    override type Data = DataFrame
    override def write(self: DataFrame, sink: DataFrameSink): Unit = {
      self.write
        .partitionBy(sink.partitionBy: _*)
        .format(sink.format)
        .options(sink.formatOptions)
        .mode(sink.mode)
        .save(sink.path)
    }
  }

  trait WriterInstances {
    implicit val dataFrameWriter: Writer.Aux[DataFrameSink, DataFrame] = DataFrameWriter
  }

  trait Instances extends WriterInstances with ReaderInstances

  object instances extends Instances
}
