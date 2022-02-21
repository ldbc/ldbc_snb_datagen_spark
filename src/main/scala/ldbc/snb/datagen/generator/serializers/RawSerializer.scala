package ldbc.snb.datagen.generator.serializers

import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.generators.{GenActivity, PersonActivityGenerator, SparkRanker}
import ldbc.snb.datagen.generator.{DatagenContext, DatagenParams}
import ldbc.snb.datagen.io.Writer
import ldbc.snb.datagen.io.raw.csv.CsvRowEncoder
import ldbc.snb.datagen.io.raw.parquet.ParquetRowEncoder
import ldbc.snb.datagen.io.raw.{RawSerializationJobContext, RawSink, recordOutputStream}
import ldbc.snb.datagen.model.raw._
import ldbc.snb.datagen.model.{EntityTraits, raw}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util
import java.util.function.Consumer
import java.util.Collections

class RawSerializer(ranker: SparkRanker)(implicit spark: SparkSession) extends Writer[RawSink] with Logging {
  override type Data = RDD[Person]

  private def writeDynamicSubgraph(persons: RDD[Person], sink: RawSink): Unit = {

    val blockSize = DatagenParams.blockSize
    val blocks = ranker(persons)
      .map { case (k, v) => (k / blockSize, v) }
      .groupByKey()

    val job = RawSerializationJobContext(persons.sparkContext.hadoopConfiguration, sink)

    job.run(blocks)((groups, wc) => {
      DatagenContext.initialize(sink.conf)

      def stream[T <: Product: EntityTraits: CsvRowEncoder: ParquetRowEncoder] =
        recordOutputStream(sink, wc)

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

      val generator = new PersonActivityGenerator

      (activityStream, pos) use { case (activityStream, pos) =>
        for { (blockId, persons) <- groups } {
          val personList = new util.ArrayList[Person](persons.size)
          for (p <- persons) {
            personList.add(p)
          }
          Collections.sort(personList)

          personList.forEach(new Consumer[Person] {
            override def accept(t: Person): Unit = pos.write(t)
          })

          val activities = generator.generateActivityForBlock(blockId.toInt, personList)

          activities.forEach(new Consumer[GenActivity] {
            override def accept(t: GenActivity): Unit = activityStream.write(t)
          })
        }
      }
    })
  }

  private def writeStaticSubgraph(persons: RDD[Person], sink: RawSink): Unit = {
    val job = RawSerializationJobContext(persons.sparkContext.hadoopConfiguration, sink)
    // we need to do this in an executor to get a TaskContext
    job.run(persons.sparkContext.parallelize(Seq(0), 1))((_, wc) => {
      DatagenContext.initialize(sink.conf)

      def stream[T <: Product: EntityTraits: CsvRowEncoder: ParquetRowEncoder] =
        recordOutputStream(sink, wc)

      import ldbc.snb.datagen.io.raw.instances._
      import ldbc.snb.datagen.model.raw.instances._
      import ldbc.snb.datagen.util.sql._

      val staticStream = new StaticOutputStream(
        stream[Place],
        stream[Tag],
        stream[TagClass],
        stream[Organisation]
      )

      staticStream use {
        _.write(StaticGraph)
      }
    })
  }

  override def write(self: RDD[Person], sink: RawSink): Unit = {
    writeDynamicSubgraph(self, sink)
    writeStaticSubgraph(self, sink)
  }
}
