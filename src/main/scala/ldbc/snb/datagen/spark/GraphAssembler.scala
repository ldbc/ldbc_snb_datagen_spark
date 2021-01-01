package ldbc.snb.datagen.spark

import ldbc.snb.datagen.entities.dynamic.Activity
import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.model.EntityType.Node
import ldbc.snb.datagen.model.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}

import scala.reflect.{ClassTag, classTag}


object GraphAssembler {
  def apply(personEntities: RDD[Person], activityEntities: RDD[Activity])(implicit spark: SparkSession): Graph[DataFrame] = {

//    import ldbc.snb.datagen.entities.EntityConversion.ops._
//    import ldbc.snb.datagen.entities.EntityConversionInstances._
//    import ldbc.snb.datagen.spark.encoder._

    import spark.implicits._

    implicit def encoderForJBean[A: ClassTag] = Encoders.bean(classTag.runtimeClass).asInstanceOf[Encoder[A]]

    val legacyPersons = spark.createDataset(personEntities).toDF
    val legacyActivities = spark.createDataset(activityEntities).toDF

    Graph("Legacy", Map(
      Node("Person") -> legacyPersons,
      Node("Activity") -> legacyActivities
    ))
  }
}
