package ldbc.snb.datagen.spark

import java.io.ObjectOutputStream
import java.util
import java.util.Properties

import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.{DatagenContext, DatagenParams}
import ldbc.snb.datagen.generator.generators.PersonGenerator
import ldbc.snb.datagen.hadoop.HadoopConfiguration
import ldbc.snb.datagen.spark.generators.SparkPersonGenerator
import ldbc.snb.datagen.util.{ConfigParser, LdbcConfiguration}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class SerializationTest extends FunSuite with BeforeAndAfterAll with Matchers {

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
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrationRequired", "true")
      .config(new SparkConf().registerKryoClasses(Array(
        classOf[Person],
        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage")
      )))
      .getOrCreate()

  }
  override def afterAll(): Unit = {
    spark.close()
    super.afterAll()
  }


  test("Serialization") {
//    val confMap: util.Map[String, String] = ConfigParser.defaultConfiguration
//    val conf = new LdbcConfiguration(confMap)
//
//    confMap.putAll(ConfigParser.readConfig(getClass.getResourceAsStream("/params_default.ini")))
//
//    DatagenContext.initialize(conf)

    val actual = SparkPersonGenerator(conf, numPartitions = Some(Integer.parseInt(hadoopConf.get("hadoop.numThreads"))))(spark)

//    val gen = new PersonGenerator(conf, DatagenParams.getDegreeDistribution.getClass.getName)
//
//    val person = gen.generatePersonBlock(0, 1000).next()
//
//    //val oos = new ObjectOutputStream()
  }

}
