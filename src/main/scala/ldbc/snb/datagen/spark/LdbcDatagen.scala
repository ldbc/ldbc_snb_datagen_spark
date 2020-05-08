package ldbc.snb.datagen.spark

import ldbc.snb.datagen.{DatagenContext, DatagenParams}
import ldbc.snb.datagen.spark.generators.{SparkActivitySerializer, SparkKnowsGenerator, SparkKnowsMerger, SparkPersonGenerator, SparkPersonSerializer, SparkRanker, SparkStaticGraphSerializer}
import ldbc.snb.datagen.util.{ConfigParser, LdbcConfiguration}
import org.apache.spark.sql.SparkSession

object LdbcDatagen {
  val appName = "LDBC Datagen for Spark"

  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()

    val start = System.currentTimeMillis

    val conf = ConfigParser.defaultConfiguration()

    conf.putAll(ConfigParser.readConfig(args(0)))
    conf.putAll(ConfigParser.readConfig(getClass.getResourceAsStream("/params_default.ini")))

    val config = new LdbcConfiguration(conf)

    val numPartitions = config.getInt("hadoop.numThreads", spark.sparkContext.defaultParallelism)

    DatagenContext.initialize(config)

    val persons = SparkPersonGenerator(config, Some(numPartitions))

    val percentages = Seq(0.45f, 0.45f, 0.1f)
    val knowsGeneratorClassName = DatagenParams.getKnowsGenerator

    import Keys._

    val uniRanker = SparkRanker.create(_.byUni, Some(numPartitions))
    val interestRanker = SparkRanker.create(_.byInterest, Some(numPartitions))
    val randomRanker = SparkRanker.create(_.byRandomId, Some(numPartitions))



    val uniKnows = SparkKnowsGenerator(persons, uniRanker, config, percentages, 0, knowsGeneratorClassName)
    val interestKnows = SparkKnowsGenerator(persons, interestRanker, config, percentages, 1, knowsGeneratorClassName)
    val randomKnows = SparkKnowsGenerator(persons, randomRanker, config, percentages, 2, knowsGeneratorClassName)

    val merged = SparkKnowsMerger(uniKnows, interestKnows, randomKnows)

    SparkActivitySerializer(merged, randomRanker, config, Some(numPartitions))

    SparkPersonSerializer(merged, config, Some(numPartitions))

    SparkStaticGraphSerializer(config, Some(numPartitions))

    print("Total Execution time: " + ((System.currentTimeMillis - start) / 1000))
  }
}



