package ldbc.snb.datagen.io.raw

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

  class CsvRecordOutputStream[T: CsvRowEncoder](
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
}
