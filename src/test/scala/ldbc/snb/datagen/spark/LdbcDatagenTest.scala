package ldbc.snb.datagen.spark

import java.util
import java.util.{Map, Properties}

import ldbc.snb.datagen._
import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.hadoop.HadoopConfiguration
import ldbc.snb.datagen.hadoop.generator.HadoopPersonGenerator
import ldbc.snb.datagen.hadoop.key.TupleKey
import ldbc.snb.datagen.spark.generators.SparkPersonGenerator
import ldbc.snb.datagen.util.{ConfigParser, LdbcConfiguration}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.SequenceFileInputFormat
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class LdbcDatagenTest extends FunSuite with BeforeAndAfterAll with Matchers {

  var hadoopConf: Configuration = _
  var hadoopPrefix: String = _
  var conf: LdbcConfiguration = _
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val confMap: util.Map[String, String] = ConfigParser.defaultConfiguration

    confMap.putAll(ConfigParser.readConfig(getClass.getResourceAsStream("/params_default.ini")))

    val props = new Properties();
    props.setProperty("ldbc.snb.datagen.generator.scaleFactor", "snb.interactive.0.1")
    props.setProperty("generator.mode", "interactive")
    props.setProperty("generator.blockSize", "100")
    props.setProperty("generator.interactive.numUpdateStreams", "1")
    props.setProperty("hadoop.numThreads", "2")

    confMap.putAll(ConfigParser.readConfig(props))

    hadoopConf = HadoopConfiguration.prepare(confMap)
    hadoopPrefix = HadoopConfiguration.getHadoopDir(hadoopConf)
    conf = HadoopConfiguration.extractLdbcConfig(hadoopConf)
    DatagenContext.initialize(conf)

    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()

  }
  override def afterAll(): Unit = {
    spark.close()
    super.afterAll()
  }

  test("Person generator returns expected results") {
    timed(
      "hadoop generation",
      new HadoopPersonGenerator(hadoopConf)
        .run(hadoopPrefix + "/persons", "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter")
    )

    val expected = spark.sparkContext.hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](hadoopPrefix + "/persons")
    val actual = SparkPersonGenerator(conf, numPartitions = Some(Integer.parseInt(hadoopConf.get("hadoop.numThreads"))))(spark)

    expected.map { case (_, v) => v.getAccountId }.collect() should contain theSameElementsAs actual.map(_.getAccountId()).collect()
  }

  def timed[A](name: String, thunk: => A): A = {
    val start = System.currentTimeMillis();
    val res = thunk
    val end = System.currentTimeMillis()
    println(s"$name: ${(end.toFloat-start) / 1000f} s")
    res
  }
}

