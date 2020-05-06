package ldbc.snb.datagen.spark.generators

import java.util

import ldbc.snb.datagen.{DatagenContext, DatagenMode, DatagenParams}
import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.generators.PersonActivityGenerator
import ldbc.snb.datagen.serializer.{DeleteEventSerializer, InsertEventSerializer, PersonActivityExporter}
import ldbc.snb.datagen.spark.util.SerializableConfiguration
import ldbc.snb.datagen.util.LdbcConfiguration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkActivitySerializer {

  def apply(persons: RDD[Person], ranker: SparkRanker, conf: LdbcConfiguration)(implicit spark: SparkSession) = {
    val blockSize = DatagenParams.blockSize
    val blocks = ranker(persons)
      .map { case (k, v) => (k / blockSize, v) }
      .groupByKey()

    val serializableHadoopConf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)

    blocks.foreachPartition(groups => {
      DatagenContext.initialize(conf)
      val partitionId = TaskContext.get().partitionId()
      val hadoopConf = serializableHadoopConf.value
      val buildDir = conf.getBuildDir

      val fs = FileSystem.get(hadoopConf)
      fs.mkdirs(new Path(buildDir))

      val dynamicActivitySerializer = conf.getDynamicActivitySerializer

      dynamicActivitySerializer.initialize(hadoopConf, conf.getSocialNetworkDir, partitionId, conf.isCompressed, conf.insertTrailingSeparator())

      var insertEventSerializer: InsertEventSerializer = null
      var deleteEventSerializer: DeleteEventSerializer = null

      if (DatagenParams.getDatagenMode != DatagenMode.RAW_DATA) {
        insertEventSerializer = new InsertEventSerializer(
          hadoopConf,
          buildDir + "/temp_insertStream_forum_" + partitionId,
          partitionId,
          DatagenParams.numUpdateStreams
        )
        deleteEventSerializer = new DeleteEventSerializer(
          hadoopConf,
          buildDir + "/temp_deleteStream_forum_" + partitionId,
          partitionId,
          DatagenParams.numUpdateStreams
        )
      }
      val generator = new PersonActivityGenerator
      val exporter = new PersonActivityExporter(dynamicActivitySerializer, insertEventSerializer, deleteEventSerializer)

      val personFactors = fs.create(new Path(buildDir + "/" + "m" + partitionId + DatagenParams.PERSON_COUNTS_FILE))
      val activityFactors = fs.create(new Path(buildDir + "/" + "m" + partitionId + DatagenParams.ACTIVITY_FILE))
      val friends = fs.create(new Path(buildDir + "/" + "m0friendList" + partitionId + ".csv"))

      for {(blockId, persons) <- groups} {
        val clonedPersons = new util.ArrayList[Person]
        for (p <- persons) {
          clonedPersons.add(new Person(p))
        }

        val activities = generator.generateActivityForBlock(blockId.toInt, clonedPersons)

        activities.forEach(exporter.export(_))

        generator.writePersonFactors(personFactors)
      }

      generator.writeActivityFactors(activityFactors)
    })
  }
}
