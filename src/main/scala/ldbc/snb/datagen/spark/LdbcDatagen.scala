package ldbc.snb.datagen.spark

import ldbc.snb.datagen.{DatagenContext, DatagenParams}
import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.spark.generators.{SparkKnowsGenerator, SparkKnowsMerger, SparkPersonGenerator}
import ldbc.snb.datagen.util.{ConfigParser, LdbcConfiguration}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object LdbcDatagen {
  val appName = "LDBC Datagen for Spark"

  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()

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

    val uniKnows = SparkKnowsGenerator(persons, config, percentages, 0, _.byUni,
      knowsGeneratorClassName, Some(numPartitions))

    val interestKnows = SparkKnowsGenerator(persons, config, percentages, 1, _.byInterest,
      knowsGeneratorClassName, Some(numPartitions))

    val randomKnows = SparkKnowsGenerator(persons, config, percentages, 2, _.byUni,
      knowsGeneratorClassName, Some(numPartitions))

    val merged = SparkKnowsMerger(uniKnows, interestKnows, randomKnows)



//    val interestKnows = genInterestKnows()
//    val randomKnows = genRandomKnows()
//
//    val knows = uniKnows ++ interestKnows ++ randomKnows
//
//    val activity = genActivity()
//
//    writeStaticGraph(persons, knows)
//
//    writeActivity(activity)

  }
}



