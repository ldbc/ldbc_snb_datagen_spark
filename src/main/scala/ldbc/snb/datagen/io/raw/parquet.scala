package ldbc.snb.datagen.io.raw

import ldbc.snb.datagen.syntax.fluentSyntaxOps
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api.RecordConsumer
import org.apache.spark.sql.{Encoder, SQLContext}
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.parquet.{LdbcDatagenParquetWriteSupport, ParquetOptions}
import org.apache.spark.sql.internal.SQLConf

object parquet {

  trait ParquetRowEncoder[T] {
    def encoder: Encoder[T]
  }
  object ParquetRowEncoder {
    implicit def apply[T: ParquetRowEncoder]: ParquetRowEncoder[T] = implicitly[ParquetRowEncoder[T]]
  }

  trait ParquetRowEncoderInstances {
    implicit def parquetRowEncoderForEncoder[A: Encoder]: ParquetRowEncoder[A] = new ParquetRowEncoder[A] {
      override def encoder: Encoder[A] = implicitly[Encoder[A]]
    }
  }

  private def parquetWriteSupportForEncodable[T <: Product: Encoder](compressionCodecClassName: Option[String]) = new WriteSupport[T] {
    private val encoder                                                        = encoderFor[T]
    private val serializer                                                     = encoder.createSerializer()
    private val schema                                                         = encoder.schema
    private val inner                                                          = new LdbcDatagenParquetWriteSupport(schema, compressionCodecClassName)
    override def init(configuration: Configuration): WriteSupport.WriteContext = inner.init(configuration)
    override def prepareForWrite(recordConsumer: RecordConsumer): Unit         = inner.prepareForWrite(recordConsumer)
    override def write(record: T): Unit                                        = inner.write(serializer.apply(record))
  }

  class ParquetRecordOutputStream[T <: Product: ParquetRowEncoder](
                                                                    path: Path,
                                                                    taskAttemptContext: TaskAttemptContext,
                                                                    options: Map[String, String]
                                                                  ) extends RecordOutputStream[T] {
    lazy val writer: RecordWriter[Void, T] = {
      val compressionCodecClassName = options
        .get(ParquetOutputFormat.COMPRESSION)
        .map(ParquetOptions.getParquetCompressionCodecName)

      implicit val encoder: Encoder[T] = implicitly[ParquetRowEncoder[T]].encoder
      val parquetWriteSupport          = parquetWriteSupportForEncodable[T](compressionCodecClassName)
      val pof                          = new ParquetOutputFormat[T](parquetWriteSupport)
      pof.getRecordWriter(taskAttemptContext, path)
    }
    var hasWritten = false

    override def write(t: T): Unit = { hasWritten = true; writer.write(null, t) }

    override def close(): Unit = if (hasWritten) { writer.close(taskAttemptContext) }
  }
}
