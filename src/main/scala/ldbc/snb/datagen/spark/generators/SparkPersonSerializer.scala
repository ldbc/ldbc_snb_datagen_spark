package ldbc.snb.datagen.spark.generators

import ldbc.snb.datagen.dictionary.Dictionaries
import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.serializer.{DeleteEventSerializer, InsertEventSerializer}
import ldbc.snb.datagen.spark.util.SerializableConfiguration
import ldbc.snb.datagen.util.LdbcConfiguration
import ldbc.snb.datagen.{DatagenContext, DatagenMode, DatagenParams}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

import ldbc.snb.datagen.spark.util.FluentSyntax._

object SparkPersonSerializer {

  def apply(
    persons: RDD[Person],
    conf: LdbcConfiguration,
    partitions: Option[Int] = None
  )(implicit spark: SparkSession): Unit = {
    val serializableHadoopConf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)

    persons
      .withFoldLeft(partitions, (rdd: RDD[Person], p: Int) => rdd.repartition(p))
      .foreachPartition(persons => {
      val dynamicPersonSerializer = conf.getDynamicPersonSerializer
      val hadoopConf = serializableHadoopConf.value
      val partitionId = TaskContext.getPartitionId()
      val buildDir = conf.getBuildDir

      val fs = FileSystem.get(hadoopConf)
      fs.mkdirs(new Path(buildDir))

      dynamicPersonSerializer.initialize(
        hadoopConf,
        conf.getSocialNetworkDir,
        partitionId,
        conf.isCompressed,
        conf.insertTrailingSeparator()
      )

      var insertEventSerializer: InsertEventSerializer = null
      var deleteEventSerializer: DeleteEventSerializer = null

      if ((DatagenParams.getDatagenMode eq DatagenMode.INTERACTIVE) || (DatagenParams.getDatagenMode eq DatagenMode.BI)) {
        insertEventSerializer = new InsertEventSerializer(hadoopConf, conf.getBuildDir + "/temp_insertStream_person_" + partitionId, partitionId, DatagenParams.numUpdateStreams)
        deleteEventSerializer = new DeleteEventSerializer(hadoopConf, conf.getBuildDir + "/temp_deleteStream_person_" + partitionId, partitionId, DatagenParams.numUpdateStreams)
      }

      try {
        DatagenContext.initialize(conf)

        for {p <- persons} {
          if (DatagenParams.getDatagenMode eq DatagenMode.RAW_DATA) {
            dynamicPersonSerializer.export(p)
            for (k <- p.getKnows.iterator.asScala) {
              dynamicPersonSerializer.export(p, k)
            }
          }
          else {
            if (p.getCreationDate < Dictionaries.dates.getBulkLoadThreshold && (p.getDeletionDate >= Dictionaries.dates.getBulkLoadThreshold && p.getDeletionDate <= Dictionaries.dates.getSimulationEnd)) {
              dynamicPersonSerializer.export(p)
              if (p.isExplicitlyDeleted) {
                deleteEventSerializer.export(p)
                deleteEventSerializer.changePartition()
              }
            }
            else if (p.getCreationDate < Dictionaries.dates.getBulkLoadThreshold && p.getDeletionDate > Dictionaries.dates.getSimulationEnd) dynamicPersonSerializer.export(p)
            else if (p.getCreationDate >= Dictionaries.dates.getBulkLoadThreshold && (p.getDeletionDate >= Dictionaries.dates.getBulkLoadThreshold) && p.getDeletionDate <= Dictionaries.dates.getSimulationEnd) {
              insertEventSerializer.export(p)
              insertEventSerializer.changePartition()
              if (p.isExplicitlyDeleted) {
                deleteEventSerializer.export(p)
                deleteEventSerializer.changePartition()
              }
            }
            else if (p.getCreationDate >= Dictionaries.dates.getBulkLoadThreshold && p.getDeletionDate > Dictionaries.dates.getSimulationEnd) {
              insertEventSerializer.export(p)
              insertEventSerializer.changePartition()
            }
            //TODO: export was split between here and HadoopPersonActivityGenerator, not sure why
            // moved all here

            for (k <- p.getKnows.iterator.asScala) {
              if (k.getCreationDate < Dictionaries.dates.getBulkLoadThreshold && (k.getDeletionDate >= Dictionaries.dates.getBulkLoadThreshold && k.getDeletionDate <= Dictionaries.dates.getSimulationEnd)) {
                dynamicPersonSerializer.export(p, k)
                if (k.isExplicitlyDeleted) {
                  deleteEventSerializer.export(p, k)
                  deleteEventSerializer.changePartition()
                }
              }
              else if (k.getCreationDate < Dictionaries.dates.getBulkLoadThreshold && k.getDeletionDate > Dictionaries.dates.getSimulationEnd) dynamicPersonSerializer.export(p, k)
              else if (k.getCreationDate >= Dictionaries.dates.getBulkLoadThreshold && (k.getDeletionDate >= Dictionaries.dates.getBulkLoadThreshold) && k.getDeletionDate <= Dictionaries.dates.getSimulationEnd) {
                insertEventSerializer.export(p, k)
                insertEventSerializer.changePartition()
                if (k.isExplicitlyDeleted) {
                  deleteEventSerializer.export(p, k)
                  deleteEventSerializer.changePartition()
                }
              }
              else if (k.getCreationDate >= Dictionaries.dates.getBulkLoadThreshold && k.getDeletionDate > Dictionaries.dates.getSimulationEnd) {
                insertEventSerializer.export(p, k)
                insertEventSerializer.changePartition()
              }
            }
          }
        }
      } finally {
        dynamicPersonSerializer.close()
        if (insertEventSerializer != null) {
          insertEventSerializer.close()
        }
        if (deleteEventSerializer != null) {
          deleteEventSerializer.close()
        }
      }
    })
  }
}
