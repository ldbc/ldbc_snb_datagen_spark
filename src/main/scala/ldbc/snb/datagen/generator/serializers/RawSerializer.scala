package ldbc.snb.datagen.generator.serializers

import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.generators.{GenActivity, PersonActivityGenerator, SparkRanker}
import ldbc.snb.datagen.generator.{DatagenContext, DatagenParams}
import ldbc.snb.datagen.io.Writer
import ldbc.snb.datagen.io.raw.csv.CsvRowEncoder
import ldbc.snb.datagen.io.raw.parquet.ParquetRowEncoder
import ldbc.snb.datagen.io.raw.{RawSink, WriteContext, createNewWriteContext, recordOutputStream}
import ldbc.snb.datagen.model.raw._
import ldbc.snb.datagen.model.{EntityTraits, raw}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable
import java.net.URI
import java.util
import java.util.Collections
import java.util.function.Consumer

class RawSerializer(ranker: SparkRanker)(implicit spark: SparkSession) extends Writer[RawSink] with Logging {
  override type Data = RDD[Person]
  import RawSerializer._

  private def writePersonSubgraph(self: RDD[Person], sink: RawSink): Unit = {
    val serializableHadoopConf = new SerializableConfiguration(self.sparkContext.hadoopConfiguration)

    self
      .pipeFoldLeft(sink.partitions)((rdd: RDD[Person], p: Int) => rdd.coalesce(p))
      .foreachPartition(persons => {
        val ctx = initializeContext(serializableHadoopConf.value, sink)

        def stream[T <: Product: EntityTraits: CsvRowEncoder: ParquetRowEncoder] =
          recordOutputStream(sink, ctx)

        import ldbc.snb.datagen.io.raw.instances._
        import ldbc.snb.datagen.model.raw.instances._
        import ldbc.snb.datagen.util.sql._

        val pos = new PersonOutputStream(
          stream[raw.Person],
          stream[raw.PersonKnowsPerson],
          stream[raw.PersonHasInterestTag],
          stream[raw.PersonStudyAtUniversity],
          stream[raw.PersonWorkAtCompany]
        )

        pos use { pos => persons.foreach(pos.write) }
      })
  }

  private def writeActivitySubgraph(persons: RDD[Person], sink: RawSink): Unit = {

    val blockSize = DatagenParams.blockSize
    val blocks = ranker(persons)
      .map { case (k, v) => (k / blockSize, v) }
      .groupByKey()
      .pipeFoldLeft(sink.partitions)((rdd: RDD[(Long, Iterable[Person])], p: Int) => rdd.coalesce(p))

    val serializableHadoopConf = new SerializableConfiguration(persons.sparkContext.hadoopConfiguration)

    blocks.foreachPartition(groups => {
      val ctx = initializeContext(serializableHadoopConf.value, sink)

      def stream[T <: Product: EntityTraits: CsvRowEncoder: ParquetRowEncoder] =
        recordOutputStream(sink, ctx)

      import ldbc.snb.datagen.io.raw.instances._
      import ldbc.snb.datagen.model.raw.instances._
      import ldbc.snb.datagen.util.sql._

      val generator = new PersonActivityGenerator
      val activityStream = new ActivityOutputStream(
        stream[Forum],
        stream[ForumHasTag],
        stream[ForumHasMember],
        stream[Post],
        stream[PostHasTag],
        stream[Comment],
        stream[CommentHasTag],
        stream[PersonLikesPost],
        stream[PersonLikesComment]
      )

      activityStream use { activityStream =>
        for { (blockId, persons) <- groups } {
          val personList = new util.ArrayList[Person](persons.size)
          for (p <- persons) { personList.add(p) }
          Collections.sort(personList)

          val activities = generator.generateActivityForBlock(blockId.toInt, personList)

          activities.forEach(new Consumer[GenActivity] {
            override def accept(t: GenActivity): Unit = activityStream.write(t)
          })
        }
      }
    })
  }

  private def writeStaticSubgraph(persons: RDD[Person], sink: RawSink): Unit = {
    val serializableHadoopConf = new SerializableConfiguration(persons.sparkContext.hadoopConfiguration)
    val sqlConf                = spark.sessionState.conf

    // we need to do this in an executor to get a TaskContext
    persons.sparkContext
      .parallelize(Seq(0), 1)
      .foreachPartition(_ => {
        val ctx = initializeContext(serializableHadoopConf.value, sink)

        def stream[T <: Product: EntityTraits: CsvRowEncoder: ParquetRowEncoder] =
          recordOutputStream(sink, ctx)

        import ldbc.snb.datagen.io.raw.instances._
        import ldbc.snb.datagen.model.raw.instances._
        import ldbc.snb.datagen.util.sql._

        val staticStream = new StaticOutputStream(
          stream[Place],
          stream[Tag],
          stream[TagClass],
          stream[Organisation]
        )

        staticStream use { _.write(StaticGraph) }
      })
  }

  override def write(self: RDD[Person], sink: RawSink): Unit = {
    writePersonSubgraph(self, sink)
    writeActivitySubgraph(self, sink)
    writeStaticSubgraph(self, sink)
  }
}
object RawSerializer {
  private def initializeContext(hadoopConf: Configuration, sink: RawSink): WriteContext = {
    DatagenContext.initialize(sink.conf)
    val buildDir = sink.conf.getOutputDir
    val fs       = FileSystem.get(new URI(buildDir), hadoopConf)
    val ctx      = createNewWriteContext(hadoopConf, fs)

    fs.mkdirs(new Path(buildDir))

    ctx
  }
}
