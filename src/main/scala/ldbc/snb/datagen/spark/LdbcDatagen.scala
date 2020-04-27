package ldbc.snb.datagen.spark

import ldbc.snb.datagen.DatagenContext
import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.spark.generators.{SparkKnowsGenerator, SparkPersonGenerator}
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

    DatagenContext.initialize(config)

    val persons = genPersons(config)
//    val uniKnows = genUniKnows()
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

  def genPersons(conf: LdbcConfiguration)(implicit spark: SparkSession): RDD[Person] = {
    SparkPersonGenerator(conf)
  }

//  def genUniKnows(conf: LdbcConfiguration)(implicit spark: SparkSession): RDD[Person] = {
//    PersonSparkKnowsGenerator(spark, conf)
//  }
}



