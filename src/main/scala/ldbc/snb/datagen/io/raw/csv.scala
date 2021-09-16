package ldbc.snb.datagen.io.raw

import ldbc.snb.datagen.entities.raw.Person
import ldbc.snb.datagen.util.formatter.DateFormatter

import java.io.OutputStream
import java.nio.charset.{Charset, StandardCharsets}

object csv {
  trait CsvRowEncoder[T] {
    def row(t: T): Seq[String]
    def header: Seq[String]
  }

  object CsvRowEncoder {
    private val dateFormatter = new DateFormatter

    private val dates = Seq("creationDate", "deletionDate", "explicitlyDeleted")

    implicit object PersonCsvRowEncoder extends CsvRowEncoder[Person] {
      override def row(person: Person): Seq[String] = Array(
        dateFormatter.formatDateTime(person.creationDate),
        dateFormatter.formatDateTime(person.deletionDate),
        String.valueOf(person.explicitlyDeleted),
        person.id.toString,
        person.firstName,
        person.lastName,
        person.gender,
        dateFormatter.formatDate(person.birthday),
        person.`locationIP`,
        person.`browserUsed`,
        Integer.toString(person.`place`),
        person.`language`.mkString(";"),
        person.`email`.mkString(";")
      )

      override def header = dates ++ Seq("id", "firstName", "lastName", "gender", "birthday", "locationIP", "browserUsed", "place", "language", "email")
    }
  }

  class CsvRecordOutputStream[T: CsvRowEncoder](
                                                 outputStream: OutputStream,
                                                 separator: String = "|",
                                                 charset: Charset = StandardCharsets.UTF_8,
                                                 writeHeader: Boolean = true
                          ) extends RecordOutputStream[T] {
    private val buffer = new StringBuilder
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
