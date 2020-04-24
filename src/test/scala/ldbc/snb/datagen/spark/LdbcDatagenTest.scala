package ldbc.snb.datagen.spark

import java.io.{PrintStream, PrintWriter}
import java.{lang, util}
import java.util.Properties

import ldbc.snb.datagen._
import ldbc.snb.datagen.entities.dynamic.person.{IP, Person, PersonSummary}
import ldbc.snb.datagen.hadoop.HadoopConfiguration
import ldbc.snb.datagen.hadoop.generator.{HadoopKnowsGenerator, HadoopPersonGenerator}
import ldbc.snb.datagen.hadoop.key.TupleKey
import ldbc.snb.datagen.spark.generators.{SparkKnowsGenerator, SparkPersonGenerator}
import ldbc.snb.datagen.util.{ConfigParser, LdbcConfiguration}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.SequenceFileInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.JavaConverters._

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
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

  }
  override def afterAll(): Unit = {
    spark.close()
    super.afterAll()
  }

  test("Person generator returns expected results") {
    timed(
      "hadoop person generation",
      new HadoopPersonGenerator(hadoopConf)
        .run(hadoopPrefix + "/persons", "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter")
    )

    val expected = spark.sparkContext.hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](hadoopPrefix + "/persons")
    val actual = SparkPersonGenerator(conf, numPartitions = Some(Integer.parseInt(hadoopConf.get("hadoop.numThreads"))))(spark)

    val actuals = actual.map(_.hashCode()).collect().toSet
    val expecteds = expected.map(_._2.hashCode()).collect().toSet

    actuals shouldBe expecteds
  }

  test("Knows generator returns expected results") {
    val personsFixturePath = getClass.getResource("/fixtures/hadoop/persons").toString

    val knowsGeneratorClassName = "ldbc.snb.datagen.generator.generators.knowsgenerators.DistanceKnowsGenerator"
    val preKeyHadoop = "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter"
    val postKeySetter = "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter"
    val stepIndex = 0

    val percentages = Seq(0.45f, 0.45f, 0.1f)
    timed(
      "hadoop knows generation",
      new HadoopKnowsGenerator(
        hadoopConf,
        preKeyHadoop,
        postKeySetter,
        percentages.map(new lang.Float(_)).asJava,
        stepIndex,
        knowsGeneratorClassName
      ).run(personsFixturePath, hadoopPrefix + "/knows")
    )

    val persons = spark.sparkContext
      .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](personsFixturePath)
      .values

    import Keys._

    implicit val sparkSession = spark

    val actual = SparkKnowsGenerator(persons, conf, percentages, stepIndex,
      sortBy = _.byUni, knowsGeneratorClassName)

    val expected = spark.sparkContext
      .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](hadoopPrefix + "/knows")

    val expecteds = expected.map { case (_, p) => p.hashCode() }.collect().toSet
    val actuals = actual.map(_.hashCode()).collect().toSet

    actuals shouldBe expecteds
  }

  def timed[A](name: String, thunk: => A): A = {
    val start = System.nanoTime()
    val res = thunk
    val end = System.nanoTime()
    println(s"$name: ${(end.toFloat-start) / 1e9f} s")
    res
  }
}

