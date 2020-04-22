package ldbc.snb.datagen.spark.generators

import ldbc.snb.datagen.entities.dynamic.person.Person
import org.apache.spark.sql.SparkSession
import ldbc.snb.datagen.{DatagenContext, DatagenParams}
import ldbc.snb.datagen.generator.generators.PersonGenerator
import ldbc.snb.datagen.util.LdbcConfiguration
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object SparkPersonGenerator {

  def apply(conf: LdbcConfiguration, numPartitions: Option[Int] = None)(implicit spark: SparkSession): RDD[Person] = {
    val numBlocks = Math.ceil(DatagenParams.numPersons / DatagenParams.blockSize.toDouble).toInt

    val personPartitionGenerator = (blocks: Iterator[Int]) => {
      DatagenContext.initialize(conf)

      val personGenerator = new PersonGenerator(conf, conf.get("generator.distribution.degreeDistribution"))

      for {
        i <- blocks
        size = Math.min(DatagenParams.numPersons - DatagenParams.blockSize * i, DatagenParams.blockSize).toInt
        person <- personGenerator.generatePersonBlock(i, DatagenParams.blockSize).asScala.take(size)
      } yield person
    }

    val partitions = numPartitions.fold(spark.sparkContext.defaultParallelism)(identity)

    spark.sparkContext
      .parallelize(0 until numBlocks, partitions)
      .mapPartitions(personPartitionGenerator, preservesPartitioning = true)
  }
}
