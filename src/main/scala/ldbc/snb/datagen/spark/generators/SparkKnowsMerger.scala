package ldbc.snb.datagen.spark.generators

import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.FriendshipMerger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object SparkKnowsMerger {

  def apply(persons: RDD[Person]*)(implicit spark: SparkSession): RDD[Person] = {
    val unioned = persons
      .foldLeft(spark.sparkContext.emptyRDD[Person]) { _ union _ }
      .cache()

    unioned
      .groupBy(_.getAccountId)
      .map { case (_, p) =>
        val merged = new FriendshipMerger().apply(p.asJava)
        merged
      }
  }
}
