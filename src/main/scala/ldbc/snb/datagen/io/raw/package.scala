package ldbc.snb.datagen.io

import ldbc.snb.datagen.DatagenContext
import ldbc.snb.datagen.dictionary.Dictionaries
import ldbc.snb.datagen.entities.dynamic.person.{Person => GenPerson}
import ldbc.snb.datagen.entities.raw.Person
import ldbc.snb.datagen.io.raw.csv.{CsvRecordOutputStream, CsvRowEncoder}
import ldbc.snb.datagen.serializer.FileName
import ldbc.snb.datagen.syntax.{fluentSyntaxOps, useSyntaxForAutoClosable}
import ldbc.snb.datagen.util.{GeneratorConfiguration, SerializableConfiguration}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import java.util
import scala.collection.JavaConverters._

package object raw {

  trait RecordOutputStream[T] extends AutoCloseable {
    def write(t: T)
  }

  class RoundRobinOutputStream[T](outputStreams: Seq[RecordOutputStream[T]]) extends RecordOutputStream[T] {
    private var current = 0
    private val n = outputStreams.length

    override def write(t: T): Unit = {
      outputStreams(current).write(t)
      current = (current + 1) % n
    }

    override def close(): Unit = for { os <- outputStreams } os.close()
  }

  private def hdfsOutputStream(fs: FileSystem, fileName: Path): FSDataOutputStream = fs.create(fileName, true, 131072)

  case class RawSink(
                      partitions: Option[Int] = None,
                      conf: GeneratorConfiguration,
                      oversizeFactor: Double = 1.0
                    )

  object PersonGeneratorOutputWriter extends Writer[RawSink] {
    override type CoRet = RDD[GenPerson]

    private def getGender(gender: Int): String = if (gender == 0) "male" else "female"

    private def getLanguages(languages: util.List[Integer]): Seq[String] = {
      languages.asScala.map(x => Dictionaries.languages.getLanguageName(x))
    }

    private def getEmail(emails: util.List[String]): Seq[String] = emails.asScala

    private def getPerson(person: GenPerson) = Person(
        person.getCreationDate,
        person.getDeletionDate,
        person.isExplicitlyDeleted,
        person.getAccountId,
        person.getFirstName,
        person.getLastName,
        getGender(person.getGender),
        person.getBirthday,
        person.getIpAddress.toString,
        Dictionaries.browsers.getName(person.getBrowserId),
        person.getCityId,
        getLanguages(person.getLanguages),
        getEmail(person.getEmails)
      )

    private def recordOutputStream[T: CsvRowEncoder](
                                                      fileSystem: FileSystem,
                                                      outputDir: String,
                                                      partitionId: Long,
                                                      oversizeFactor: Double,
                                                      fileName: FileName
                                                    ) = {
      val numFiles = Math.ceil(fileName.size / oversizeFactor).toLong
      val files = for { i <- 0L until numFiles } yield {
        new Path(s"$outputDir/graphs/csv/raw/composite-merged-fk/dynamic/${fileName.name}/part_${partitionId}_${i}.csv")
      }
      new RoundRobinOutputStream(files
        .map(hdfsOutputStream(fileSystem, _))
        .map(new CsvRecordOutputStream[T](_))
      )
    }

    override def write(self: RDD[GenPerson], sink: RawSink): Unit = {
      val serializableHadoopConf = new SerializableConfiguration(self.sparkContext.hadoopConfiguration)

      self
        .pipeFoldLeft(sink.partitions)((rdd: RDD[GenPerson], p: Int) => rdd.coalesce(p))
        .foreachPartition(persons => {
          DatagenContext.initialize(sink.conf)
          val hadoopConf = serializableHadoopConf.value
          val partitionId = TaskContext.getPartitionId()
          val buildDir = sink.conf.getOutputDir
          val fs = FileSystem.get(hadoopConf)
          fs.mkdirs(new Path(buildDir))
          val ros = recordOutputStream[Person](fs, buildDir, partitionId, sink.oversizeFactor, FileName.PERSON)
          ros use { ros =>
            persons.foreach { person =>
              val p = getPerson(person)
              ros.write(p)
            }
          }
        })
    }
  }
}
