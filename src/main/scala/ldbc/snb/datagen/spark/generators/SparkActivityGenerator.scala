package ldbc.snb.datagen.spark.generators

import java.util

import ldbc.snb.datagen.{DatagenContext, DatagenParams}
import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.generators.PersonActivityGenerator
import ldbc.snb.datagen.util.LdbcConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkActivityGenerator {

  def apply(persons: RDD[Person], ranker: SparkRanker, conf: LdbcConfiguration)(implicit spark: SparkSession) = {
    val blockSize = DatagenParams.blockSize
    val blocks = ranker(persons)
      .map { case (k, v) => (k / blockSize, v) }
      .groupByKey()


    blocks.mapPartitions(groups => {
      DatagenContext.initialize(conf)

      for { (blockId, persons) <- groups } yield {
        val activityGenerator = new PersonActivityGenerator

        val clonedPersons = new util.ArrayList[Person]
        for (p <- persons) {
          clonedPersons.add(new Person(p))
        }

        val activities = activityGenerator.generateActivityForBlock(blockId.toInt, clonedPersons)


      }


      ???
    })





  }

}
