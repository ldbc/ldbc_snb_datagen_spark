package ldbc.snb.datagen.spark.generators

import ldbc.snb.datagen.entities.dynamic.person.Person
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

//object SparkKnowsGenerator {
//  def apply[K: Ordering: ClassTag](persons: RDD[Person], conf: Config, percentages: Seq[Float], stepIndex: Int, grouper: Person => K)(implicit spark: SparkSession) = {
//    persons
//      .sortBy(grouper)
//      .
//
//  }
//}
