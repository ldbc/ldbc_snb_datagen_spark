package ldbc.snb.datagen.io

import ldbc.snb.datagen.io.raw.csv.{CsvRecordOutputStream, CsvRowEncoder}
import ldbc.snb.datagen.io.raw.parquet.{ParquetRecordOutputStream, ParquetRowEncoder}
import ldbc.snb.datagen.model.EntityTraits
import ldbc.snb.datagen.util.GeneratorConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce._
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.TaskContext
import org.apache.spark.sql.Encoder

package object raw {
  trait RecordOutputStream[T] extends AutoCloseable {
    def write(t: T)
  }

  trait MakePart[T] {
    def apply(part: Int): RecordOutputStream[T]
    def exists(): Boolean
    def delete(): Unit
  }

  class MakeParquetPart[T <: Product: ParquetRowEncoder](pathPrefix: String, writeContext: WriteContext) extends MakePart[T] {
    private val compression = CompressionCodecName.fromConf(DefaultParquetCompression)
    private val options     = Map { ParquetOutputFormat.COMPRESSION -> compression.toString }
    private val partitionId = writeContext.taskContext.partitionId()
    private val extension   = s"${compression.getExtension}.parquet"

    def apply(part: Int) = {
      val path = new Path(s"${pathPrefix}/part_${partitionId}_${part}${extension}")
      new ParquetRecordOutputStream[T](path, writeContext.taskAttemptContext, options)
    }

    override def exists(): Boolean = {
      !writeContext.fileSystem.globStatus(new Path(s"${pathPrefix}/part_${partitionId}_*${extension}")).isEmpty
    }

    override def delete(): Unit = {
      val files = writeContext.fileSystem.globStatus(new Path(s"${pathPrefix}/part_${partitionId}_*${extension}"))
      for { f <- files } writeContext.fileSystem.delete(f.getPath, false)
    }
  }

  class MakeCsvPart[T <: Product: CsvRowEncoder](pathPrefix: String, writeContext: WriteContext) extends MakePart[T] {
    private val partitionId = writeContext.taskContext.partitionId()
    private val extension   = ".csv"

    def apply(part: Int) = {
      val partitionId = writeContext.taskContext.partitionId()
      val extension   = ".csv"
      val path        = new Path(s"${pathPrefix}/part_${partitionId}_${part}${extension}")
      new CsvRecordOutputStream[T](writeContext.fileSystem.create(path, true, 131072))
    }

    override def exists(): Boolean = {
      !writeContext.fileSystem.globStatus(new Path(s"${pathPrefix}/part_${partitionId}_*${extension}")).isEmpty
    }

    override def delete(): Unit = {
      val files = writeContext.fileSystem.globStatus(new Path(s"${pathPrefix}/part_${partitionId}_*${extension}"))
      for { f <- files } writeContext.fileSystem.delete(f.getPath, false)
    }
  }

  class FixedSizeBatchOutputStream[T](size: Long, makePart: MakePart[T]) extends RecordOutputStream[T] {
    private var written: Long           = 0
    private var currentBatchNumber: Int = 0
    private var currentBatch            = makePart(currentBatchNumber)

    override def write(t: T): Unit = {
      if (currentBatch == null)
        throw new AssertionError("Stream already closed")

      if (written >= size) {
        currentBatch.close()
        currentBatchNumber = currentBatchNumber + 1
        currentBatch = makePart(currentBatchNumber)
        written = 0
      }

      currentBatch.write(t)
      written = written + 1
    }

    override def close(): Unit = {
      if (currentBatch != null) {
        currentBatch.close()
        currentBatch = null
      }
    }
  }

  class RoundRobinOutputStream[T](outputStreams: Seq[RecordOutputStream[T]]) extends RecordOutputStream[T] {
    private var current = 0
    private val n       = outputStreams.length

    override def write(t: T): Unit = {
      outputStreams(current).write(t)
      current = (current + 1) % n
    }

    override def close(): Unit = for { os <- outputStreams } os.close()
  }

  sealed trait RawFormat
  case object Csv     extends RawFormat { override def toString = "csv"     }
  case object Parquet extends RawFormat { override def toString = "parquet" }

  case class RawSink(
      format: RawFormat,
      partitions: Option[Int] = None,
      conf: GeneratorConfiguration,
      oversizeFactor: Option[Double] = None,
      overwrite: Boolean = false
  )

  class WriteContext(
      val taskContext: TaskContext,
      val taskAttemptContext: TaskAttemptContext,
      val hadoopConf: Configuration,
      val fileSystem: FileSystem
  )

  // See org.apache.parquet.hadoop.metadata.CompressionCodecName for other options. Note that not all are
  // supported by Spark.
  val DefaultParquetCompression = "snappy"

  val DefaultBatchSize: Long = 10000000

  def recordOutputStream[T <: Product: EntityTraits: CsvRowEncoder: ParquetRowEncoder](sink: RawSink, writeContext: WriteContext): RecordOutputStream[T] = {
    val et                           = EntityTraits[T]
    val size                         = (DefaultBatchSize * sink.oversizeFactor.getOrElse(1.0)).toLong
    implicit val encoder: Encoder[T] = ParquetRowEncoder[T].encoder
    val entityPath                   = et.`type`.entityPath
    val pathPrefix                   = s"${sink.conf.getOutputDir}/graphs/${sink.format.toString}/raw/composite-merged-fk/${entityPath}"

    val makePart = sink.format match {
      case Parquet => new MakeParquetPart[T](pathPrefix, writeContext)
      case Csv     => new MakeCsvPart[T](pathPrefix, writeContext)
      case x       => throw new UnsupportedOperationException(s"Raw serializer not implemented for format ${x}")
    }
    makePart.delete()
    new FixedSizeBatchOutputStream[T](size, makePart)
  }

  object instances extends csv.CsvRowEncoderInstances with parquet.ParquetRowEncoderInstances
}
