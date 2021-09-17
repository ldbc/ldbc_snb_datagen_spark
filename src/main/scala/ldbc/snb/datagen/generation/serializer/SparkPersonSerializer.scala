package ldbc.snb.datagen.generation.serializer

import ldbc.snb.datagen.DatagenContext
import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.serializer.{DynamicPersonSerializer, PersonExporter}
import ldbc.snb.datagen.util.GeneratorConfiguration
import ldbc.snb.datagen.syntax._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

import java.net.URI

object SparkPersonSerializer {

  def apply(
    persons: RDD[Person],
    conf: GeneratorConfiguration,
    partitions: Option[Int] = None,
    oversizeFactor: Double = 1.0
  )(implicit spark: SparkSession): Unit = {
    val serializableHadoopConf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)

    persons
      .pipeFoldLeft(partitions)((rdd: RDD[Person], p: Int) => rdd.coalesce(p))
      .foreachPartition(persons => {
        val dynamicPersonSerializer = new DynamicPersonSerializer
        val hadoopConf = serializableHadoopConf.value
        val partitionId = TaskContext.getPartitionId()
        val buildDir = conf.getOutputDir

        val fs = FileSystem.get(new URI(buildDir), hadoopConf)
        fs.mkdirs(new Path(buildDir))

        dynamicPersonSerializer.initialize(
          fs,
          conf.getOutputDir,
          partitionId,
          oversizeFactor,
          false
        )

        val personExporter = new PersonExporter(dynamicPersonSerializer)

        personExporter use { pe =>
          DatagenContext.initialize(conf)
          for {p <- persons} {
            pe.export(p)
          }
        }
      })
  }
}
