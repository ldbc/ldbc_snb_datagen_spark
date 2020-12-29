package ldbc.snb.datagen.spark

import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.generators.GenActivity
import ldbc.snb.datagen.model.EntityType.Node
import ldbc.snb.datagen.model.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object GraphAssembler {
  def apply(personEntities: RDD[Person], activityEntities: RDD[GenActivity])(implicit spark: SparkSession): Graph[DataFrame] = {

    import ldbc.snb.datagen.entities.EntityConversion.ops._
    import ldbc.snb.datagen.entities.EntityConversionInstances._
    import ldbc.snb.datagen.spark.encoder._

    val legacyPersons = spark.createDataset(personEntities.map(_.repr)).toDF
    val legacyActivities = spark.createDataset(activityEntities.map(_.repr)).toDF

    Graph("Legacy", Map(
      Node("Person") -> legacyPersons,
      Node("Activity") -> legacyActivities
    ))
  }
}
