package ldbc.snb.datagen.spark.generators

import ldbc.snb.datagen.DatagenContext
import ldbc.snb.datagen.generator.generators.GenActivity
import ldbc.snb.datagen.serializer.{DummyDeleteEventSerializer, DummyInsertEventSerializer, PersonActivityExporter}
import ldbc.snb.datagen.spark.util.SerializableConfiguration
import ldbc.snb.datagen.util.LdbcConfiguration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object SparkActivitySerializer {

  def apply(activities: RDD[(Long, Array[GenActivity])], ranker: SparkRanker, conf: LdbcConfiguration, partitions: Option[Int] = None)(implicit spark: SparkSession) = {

    val serializableHadoopConf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)

    activities.foreachPartition(groups => {
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

      val exporter = new PersonActivityExporter(dynamicActivitySerializer, insertEventSerializer, deleteEventSerializer)

      try {
        for {(_, activities) <- groups} {
          activities.toIterator.foreach(exporter.export)
        }
      } finally {
        exporter.close()
      }
    })
  }
}
