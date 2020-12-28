package ldbc.snb.datagen.spark.generators

import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.generators.{GenActivity, PersonActivityGenerator}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.spark.util.Utils.arrayOfSize
import ldbc.snb.datagen.util.LdbcConfiguration
import ldbc.snb.datagen.{DatagenContext, DatagenParams}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object SparkActivityGenerator {
  def apply(persons: RDD[Person], ranker: SparkRanker, conf: LdbcConfiguration, partitions: Option[Int] = None)(implicit spark: SparkSession): RDD[GenActivity] = {
    val blockSize = DatagenParams.blockSize
    val blocks = ranker(persons)
      .map { case (k, v) => (k / blockSize, v) }
      .groupByKey()
      .withFoldLeft(partitions, (rdd: RDD[(Long, Iterable[Person])], p: Int) => rdd.coalesce(p))

    blocks
      .mapPartitions(groups => {
        DatagenContext.initialize(conf)
        val generator = new PersonActivityGenerator
        for {(blockId, persons) <- groups} yield {
          blockId -> generator.generateActivityForBlock(blockId.toInt, persons.toArray).toArray(arrayOfSize[GenActivity])
        }
      })
      .flatMap[GenActivity] { case (_, v) => v }
      .withFoldLeft(partitions, (rdd: RDD[GenActivity], p: Int) => rdd.repartition(p))
  }
}
