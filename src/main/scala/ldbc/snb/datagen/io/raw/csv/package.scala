package ldbc.snb.datagen.io.raw

import ldbc.snb.datagen.io.raw.combinators.MakeBatchPart
import org.apache.hadoop.fs.Path

import java.io.OutputStream
import java.nio.charset.{Charset, StandardCharsets}

package object csv {
  trait CsvRowEncoder[T] {
    def row(t: T): Seq[String]
    def header: Seq[String]
  }
  object CsvRowEncoder {
    def apply[T: CsvRowEncoder]: CsvRowEncoder[T] = implicitly[CsvRowEncoder[T]]
  }

  final class CsvRecordOutputStream[T: CsvRowEncoder](
      outputStream: OutputStream,
      separator: String = "|",
      charset: Charset = StandardCharsets.UTF_8,
      writeHeader: Boolean = true
  ) extends RecordOutputStream[T] {
    private val buffer  = new StringBuilder
    private val encoder = implicitly[CsvRowEncoder[T]]

    if (writeHeader)
      writeEntry(encoder.header)

    private def writeEntry(entry: Seq[String]) = {
      buffer.setLength(0)
      entry.addString(buffer, separator)
      buffer ++= "\n"
      outputStream.write(buffer.mkString.getBytes(charset))
    }

    override def write(t: T): Unit = writeEntry(encoder.row(t))

    override def close(): Unit = outputStream.close()
  }

  final class MakeCsvBatchPart[T <: Product: CsvRowEncoder](pathPrefix: String, writeContext: WriteContext) extends MakeBatchPart[T] {
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
}
