package ldbc.snb.datagen.spark

import java.io.File
import java.{lang, util}
import java.util.{ArrayList, Arrays, Properties}

import ldbc.snb.datagen._
import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.hadoop.HadoopConfiguration
import ldbc.snb.datagen.hadoop.generator.{HadoopKnowsGenerator, HadoopPersonGenerator}
import ldbc.snb.datagen.hadoop.key.TupleKey
import ldbc.snb.datagen.hadoop.miscjob.HadoopMergeFriendshipFiles
import ldbc.snb.datagen.spark.generators.{SparkKnowsGenerator, SparkKnowsMerger, SparkPersonGenerator}
import ldbc.snb.datagen.util.{ConfigParser, LdbcConfiguration}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.SequenceFileInputFormat
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, BeforeAndAfterEach, FunSuite, Matchers}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class LdbcDatagenScalaTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach with Matchers {

  var hadoopConf: Configuration = _
  var hadoopPrefix: String = _
  var conf: LdbcConfiguration = _
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val confMap: util.Map[String, String] = ConfigParser.defaultConfiguration

    confMap.putAll(ConfigParser.readConfig(getClass.getResourceAsStream("/params_default.ini")))

    val props = new Properties()
    props.setProperty("generator.scaleFactor", "0.1")
    props.setProperty("generator.mode", "interactive")
    props.setProperty("generator.blockSize", "100")
    props.setProperty("generator.interactive.numUpdateStreams", "1")
    props.setProperty("hadoop.numThreads", "1")

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

  override def beforeEach(): Unit = {
    val dfs = FileSystem.get(hadoopConf)
    dfs.delete(new Path(hadoopConf.get("hadoop.serializer.hadoopDir")), true)
    dfs.delete(new Path(hadoopConf.get("hadoop.serializer.socialNetworkDir")), true)
    FileUtils.deleteDirectory(new File(hadoopConf.get("hadoop.serializer.outputDir") + "/substitution_parameters"))
  }

  test("Person generator is deterministic") {
    timed(
      "hadoop person generation",
      new HadoopPersonGenerator(hadoopConf)
        .run(hadoopPrefix + "/persons", "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter")
    )

    timed(
      "hadoop person generation",
      new HadoopPersonGenerator(hadoopConf)
        .run(hadoopPrefix + "/persons2", "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter")
    )

    val expected = spark.sparkContext.hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](hadoopPrefix + "/persons")
    val actual = spark.sparkContext.hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](hadoopPrefix + "/persons2")

    val actuals = actual.map(_._2.hashCode()).collect().toSet
    val expecteds = expected.map(_._2.hashCode()).collect().toSet

    actuals shouldBe expecteds
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

  import Keys._

  test("University Knows generator returns expected results") {
    shouldGenerateSameKnows(
      "/knows_university",
      0,
      (p: Person) => p.byUni,
      "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter"
    )
  }

  test("Interest Knows generator returns expected results") {
    shouldGenerateSameKnows(
      "/knows_interest",
      1,
      (p: Person) => p.byInterest,
      "ldbc.snb.datagen.hadoop.miscjob.keychanger.InterestKeySetter"
    )
  }

  test("Random Knows generator returns expected results") {
    shouldGenerateSameKnows(
      "/knows_random",
      2,
      (p: Person) => p.byRandomId,
      "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
      knowsGeneratorClassName = "ldbc.snb.datagen.generator.generators.knowsgenerators.RandomKnowsGenerator"
    )
  }

  test("Merger returns expected results") {
    val personsFixturePath = getClass.getResource("/fixtures/hadoop").toString

    implicit val sparkSession = spark

    val uni = spark.sparkContext
      .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](personsFixturePath + "/knows_university")
      .values
      .map(new Person(_)) // this required because of https://issues.apache.org/jira/browse/SPARK-993

    val interest = spark.sparkContext
      .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](personsFixturePath + "/knows_interest")
      .values
      .map(new Person(_)) // this required because of https://issues.apache.org/jira/browse/SPARK-993

    val random = spark.sparkContext
      .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](personsFixturePath + "/knows_random")
      .values
      .map(new Person(_)) // this required because of https://issues.apache.org/jira/browse/SPARK-993

    val merger = new HadoopMergeFriendshipFiles(hadoopConf, "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter")
    timed(
      "hadoop merge",
      merger.run(
        hadoopPrefix + "/mergedPersons",
        new util.ArrayList[String](util.Arrays.asList(
          personsFixturePath + "/knows_university",
          personsFixturePath + "/knows_interest",
          personsFixturePath + "/knows_random"
        ))
      )
    )

    val actual = SparkKnowsMerger(uni, interest, random)

    val expected = spark.sparkContext
      .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](hadoopPrefix + "/mergedPersons")
      .values

    val expecteds = expected.map(_.hashCode).collect().toSet

    val actuals = actual.map(_.hashCode).collect().toSet

    actuals should have size 1700
    actuals shouldBe expecteds
  }

  def shouldGenerateSameKnows[K: ClassTag: Ordering](
    hadoopDir: String,
    stepIndex: Int,
    sparkSorter: Person => K,
    hadoopSorter: String,
    knowsGeneratorClassName: String = "ldbc.snb.datagen.generator.generators.knowsgenerators.DistanceKnowsGenerator"
  ) = {
    val personsFixturePath = getClass.getResource("/fixtures/hadoop/persons").toString

    val knowsGeneratorClassName = "ldbc.snb.datagen.generator.generators.knowsgenerators.DistanceKnowsGenerator"
    val preKeyHadoop = hadoopSorter
    val postKeySetter = "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter"

    val percentages = Seq(0.45f, 0.45f, 0.1f)
    timed(
      s"hadoop knows generation for $hadoopDir",
      new HadoopKnowsGenerator(
        hadoopConf,
        preKeyHadoop,
        postKeySetter,
        percentages.map(new lang.Float(_)).asJava,
        stepIndex,
        knowsGeneratorClassName
      ).run(personsFixturePath, hadoopPrefix + hadoopDir)
    )

    val persons = spark.sparkContext
      .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](personsFixturePath)
      .values

    implicit val sparkSession = spark

    val actual = SparkKnowsGenerator(persons, conf, percentages, stepIndex,
      sortBy = sparkSorter, knowsGeneratorClassName)

    val expected = spark.sparkContext
      .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](hadoopPrefix + hadoopDir)

    val expecteds = expected.map { case (_, p) => p.hashCode() }.collect().toSet
    val actuals = actual.map(_.hashCode()).collect().toSet

    expecteds should have size 1700
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

