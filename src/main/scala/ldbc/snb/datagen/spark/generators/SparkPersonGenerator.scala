package ldbc.snb.datagen.spark.generators

import ldbc.snb.datagen.entities.dynamic.person.Person
import org.apache.spark.sql.SparkSession
import ldbc.snb.datagen.{DatagenContext, DatagenParams}
import ldbc.snb.datagen.generator.generators.PersonGenerator
import ldbc.snb.datagen.util.LdbcConfiguration
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object SparkPersonGenerator {

  def apply(conf: LdbcConfiguration)(implicit spark: SparkSession): RDD[Person] = {
    val numBlocks = Math.ceil(DatagenParams.numPersons / DatagenParams.blockSize.toDouble).toInt
    val numThreads = Integer.parseInt(conf.get("ldbc.snb.datagen.generator.numThreads"))

    val personPartitionGenerator = (blocks: Iterator[Int]) => {
      DatagenContext.initialize(conf)

      val personGenerator = new PersonGenerator(conf, conf.get("ldbc.snb.datagen.generator.distribution.degreeDistribution"))

      for {
        i <- blocks
        size = Math.min(DatagenParams.numPersons - DatagenParams.blockSize * i, DatagenParams.blockSize).toInt
        person <- personGenerator.generatePersonBlock(i, DatagenParams.blockSize).asScala.take(size)
      } yield person
    }

    spark.sparkContext
      .parallelize(0 until numBlocks, numThreads)
      .mapPartitions(personPartitionGenerator, preservesPartitioning = true)
  }
}
