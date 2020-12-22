package ldbc.snb.datagen.spark.transform

import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.generators.GenActivity
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GraphAssembler {

  import EntityConversion.ops._
  import EntityConversionInstances._

  def apply(persons: RDD[Person], activities: RDD[(Long, Array[GenActivity])])(implicit spark: SparkSession) = {
    import spark.implicits._
    val convertedPersons = spark.createDataset(persons.mapPartitions(_.map(_.repr)))

    Graph()

  }

}
