package ldbc.snb.datagen.spark

import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.generators.GenActivity
import ldbc.snb.datagen.model.EntityType.Node
import ldbc.snb.datagen.model.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object GraphAssembler {

  import ldbc.snb.datagen.entities.EntityConversion.ops._
  import ldbc.snb.datagen.entities.EntityConversionInstances._

  def apply(personEntities: RDD[Person], activityEntities: RDD[GenActivity])(implicit spark: SparkSession): Graph[DataFrame] = {

    import ldbc.snb.datagen.spark.encoder._

    val legacyPersons = spark.createDataset(personEntities.mapPartitions(_.map(_.repr)))

    val legacyActivities = spark
      .createDataset(activityEntities.map(_.repr))

    Graph("Legacy", Map(
      Node("Person") -> legacyPersons.toDF,
      Node("Activity") -> legacyActivities.toDF
    ))
  }
}
