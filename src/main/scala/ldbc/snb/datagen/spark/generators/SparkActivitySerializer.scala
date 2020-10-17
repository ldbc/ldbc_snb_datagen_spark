package ldbc.snb.datagen.spark.generators

import java.nio.charset.StandardCharsets
import java.util
import java.util.function.Consumer

import ldbc.snb.datagen.{DatagenContext, DatagenMode, DatagenParams}
import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.generators.{GenActivity, PersonActivityGenerator}
import ldbc.snb.datagen.serializer.{DeleteEventSerializer, DummyDeleteEventSerializer, DummyInsertEventSerializer, InsertEventSerializer, PersonActivityExporter}
import ldbc.snb.datagen.spark.util.SerializableConfiguration
import ldbc.snb.datagen.util.LdbcConfiguration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import ldbc.snb.datagen.spark.util.FluentSyntax._

object SparkActivitySerializer {

  def apply(persons: RDD[Person], ranker: SparkRanker, conf: LdbcConfiguration, partitions: Option[Int] = None)(implicit spark: SparkSession) = {

    val blockSize = DatagenParams.blockSize
    val blocks = ranker(persons)
      .map { case (k, v) => (k / blockSize, v) }
      .groupByKey()
      .withFoldLeft(partitions, (rdd: RDD[(Long, Iterable[Person])], p: Int) => rdd.repartition(p))

    val serializableHadoopConf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)

    blocks.foreachPartition(groups => {
      DatagenContext.initialize(conf)
      val partitionId = TaskContext.getPartitionId()
      val hadoopConf = serializableHadoopConf.value
      val buildDir = conf.getBuildDir

      val fs = FileSystem.get(hadoopConf)
      fs.mkdirs(new Path(buildDir))

      val dynamicActivitySerializer = conf.getDynamicActivitySerializer

      dynamicActivitySerializer.initialize(hadoopConf, conf.getSocialNetworkDir, partitionId, conf.isCompressed, conf.insertTrailingSeparator())

      val insertEventSerializer = new DummyInsertEventSerializer
      val deleteEventSerializer = new DummyDeleteEventSerializer

      val generator = new PersonActivityGenerator
      val exporter = new PersonActivityExporter(dynamicActivitySerializer, insertEventSerializer, deleteEventSerializer, generator.getFactorTable)
      val friends = fs.create(new Path(buildDir + "/" + "m0friendList" + partitionId + ".csv"))
      val personFactors = fs.create(new Path(buildDir + "/" + "m" + partitionId + DatagenParams.PERSON_COUNTS_FILE))
      val activityFactors = fs.create(new Path(buildDir + "/" + "m" + partitionId + DatagenParams.ACTIVITY_FILE))

      try {
        for {(blockId, persons) <- groups} {
          val clonedPersons = new util.ArrayList[Person]
          for (p <- persons) {
            clonedPersons.add(new Person(p))

            val strbuf = new StringBuilder
            strbuf.append(p.getAccountId)
            for (k <- p.getKnows.iterator().asScala) {
              strbuf.append("|")
              strbuf.append(k.to.getAccountId)
            }
            strbuf.append("\n")
            friends.write(strbuf.toString().getBytes(StandardCharsets.UTF_8))
          }

          val activities = generator.generateActivityForBlock(blockId.toInt, clonedPersons)

          activities.forEach(new Consumer[GenActivity] {
            override def accept(t: GenActivity): Unit = exporter.export(t)
          })

          generator.writePersonFactors(personFactors)
        }
        generator.writeActivityFactors(activityFactors)
      } finally {
        exporter.close()
        friends.close()
        personFactors.close()
        activityFactors.close()
      }
    })
  }
}
