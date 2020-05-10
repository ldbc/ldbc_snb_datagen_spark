package ldbc.snb.datagen.spark

import java.lang
import java.nio.file.{Files, Paths}
import java.util.Properties

import better.files._
import ldbc.snb.datagen._
import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.hadoop.HadoopConfiguration
import ldbc.snb.datagen.hadoop.generator.{HadoopKnowsGenerator, HadoopPersonActivityGenerator, HadoopPersonGenerator}
import ldbc.snb.datagen.hadoop.key.TupleKey
import ldbc.snb.datagen.hadoop.miscjob.HadoopMergeFriendshipFiles
import ldbc.snb.datagen.hadoop.serializer.HadoopPersonSortAndSerializer
import ldbc.snb.datagen.spark.generators._
import ldbc.snb.datagen.spark.util.{SaneDefaults, SparkTesting}
import ldbc.snb.datagen.util.{ConfigParser, LdbcConfiguration}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.SequenceFileInputFormat
import org.scalatest._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class LdbcDatagenSparkRegressionTest extends FunSuite
  with SaneDefaults
  with SparkTesting
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with Matchers
  with Inspectors {

  // to reduce boilerplate
  import scala.language.implicitConversions
  implicit def convertToJavaFile(file: File): java.io.File = file.toJava
  implicit def convertToString(file: File): String = file.toString

  def withHadoopConf(propOverrides: (Properties => Properties)*)(test: (LdbcConfiguration, Configuration) => Any) = {
    val confMap = ConfigParser.defaultConfiguration
    confMap.putAll(ConfigParser.readConfig(getClass.getResourceAsStream("/params_default.ini")))

    val props = new Properties()
    props.setProperty("generator.scaleFactor", "0.1")
    props.setProperty("generator.mode", "interactive")
    props.setProperty("generator.blockSize", "100")
    props.setProperty("generator.interactive.numUpdateStreams", "1")
    props.setProperty("hadoop.numThreads", "3")

    val tmpRoot = Files.createTempDirectory("ldbc_snb_datagen_test").toString

    props.setProperty("serializer.buildDir", tmpRoot / "build")
    props.setProperty("serializer.socialNetworkDir", tmpRoot / "sn")
    props.setProperty("serializer.outputDir", tmpRoot / "out")

    val finalProps = propOverrides.foldLeft(props)((p, f) => f(p))

    confMap.putAll(ConfigParser.readConfig(finalProps))

    val conf = new LdbcConfiguration(confMap)

    DatagenContext.initialize(conf)

    val hadoopConf = HadoopConfiguration.prepare(conf)
    test(conf, hadoopConf)
  }

  val fixturePath = getClass.getResource("/fixtures/hadoop").getPath

  test("Person generator is deterministic") {
    withHadoopConf() { (conf, hadoopConf) =>
      timed(
        "hadoop person generation",
        new HadoopPersonGenerator(conf, hadoopConf)
          .run(conf.getBuildDir / "persons", "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter")
      )

      timed(
        "hadoop person generation",
        new HadoopPersonGenerator(conf, hadoopConf)
          .run(conf.getBuildDir / "persons2", "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter")
      )

      val expected = spark.sparkContext.hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](conf.getBuildDir / "persons")
      val actual = spark.sparkContext.hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](conf.getBuildDir / "persons2")

      val actuals = actual.map(_._2.hashCode()).collect().toSet
      val expecteds = expected.map(_._2.hashCode()).collect().toSet

      actuals shouldBe expecteds
    }
  }


  test("Person generator returns expected results") {
    withHadoopConf() { (conf, hadoopConf) =>
    timed(
      "hadoop person generation",
      new HadoopPersonGenerator(conf, hadoopConf)
        .run(conf.getBuildDir / "persons", "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter")
    )

    val expected = spark.sparkContext
      .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](conf.getBuildDir / "persons")
    val actual = SparkPersonGenerator(conf, numPartitions = Some(Integer.parseInt(hadoopConf.get("hadoop.numThreads"))))(spark)

    val actuals = actual.map(_.hashCode()).collect().toSet
    val expecteds = expected.map(_._2.hashCode()).collect().toSet

    actuals shouldBe expecteds
  }}

  import Keys._

  test("University Knows generator returns expected results") {
    withHadoopConf() { (conf, hadoopConf) =>
      shouldGenerateSameKnows(
        "knows_university",
        0,
        (p: Person) => p.byUni,
        "ldbc.snb.datagen.hadoop.miscjob.keychanger.UniversityKeySetter",
        conf,
        hadoopConf
      )
    }
  }

  test("Interest Knows generator returns expected results") {
    withHadoopConf() { (conf, hadoopConf) =>
      shouldGenerateSameKnows(
        "knows_interest",
        1,
        (p: Person) => p.byInterest,
        "ldbc.snb.datagen.hadoop.miscjob.keychanger.InterestKeySetter",
        conf,
        hadoopConf
      )
    }
  }

  test("Random Knows generator returns expected results") {
    withHadoopConf() { (conf, hadoopConf) =>
      shouldGenerateSameKnows(
        "knows_random",
        2,
        (p: Person) => p.byRandomId,
        "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter",
        conf,
        hadoopConf,
        knowsGeneratorClassName = "ldbc.snb.datagen.generator.generators.knowsgenerators.RandomKnowsGenerator"
      )
    }
  }

  test("Merger returns expected results") {
    val uni = spark.sparkContext
      .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](fixturePath / "knows_university")
      .values
      .map(new Person(_)) // this required because of https://issues.apache.org/jira/browse/SPARK-993

    val interest = spark.sparkContext
      .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](fixturePath / "knows_interest")
      .values
      .map(new Person(_)) // this required because of https://issues.apache.org/jira/browse/SPARK-993

    val random = spark.sparkContext
      .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](fixturePath / "knows_random")
      .values
      .map(new Person(_)) // this required because of https://issues.apache.org/jira/browse/SPARK-993

    withHadoopConf() { (conf, hadoopConf) =>
      val merger = new HadoopMergeFriendshipFiles(hadoopConf, "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter")
      timed(
        "hadoop merge",
        merger.run(
          conf.getBuildDir + "/merged_persons",
          new java.util.ArrayList(java.util.Arrays.asList[String](
            fixturePath / "knows_university",
            fixturePath / "knows_interest",
            fixturePath / "knows_random"
          ))
        )
      )

      val actual = SparkKnowsMerger(uni, interest, random)

      val expected = spark.sparkContext
        .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](conf.getBuildDir / "merged_persons")
        .values

      val expecteds = expected.map(_.hashCode).collect().toSet

      val actuals = actual.map(_.hashCode).collect().toSet

      actuals should have size 1700
      actuals shouldBe expecteds
    }
  }

  test("Person serializer generates & serializers the same persons") {
    withHadoopConf() { (conf, hadoopConf) =>
      val persons = spark.sparkContext
        .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](fixturePath / "merged_persons")
        .values

      val numPartitions = HadoopConfiguration.getNumThreads(hadoopConf)

      {
        val (h, c) = updateConf(hadoopConf,
          "serializer.buildDir" -> (conf.getBuildDir / "expected"),
          "serializer.socialNetworkDir" -> (conf.getSocialNetworkDir / "expected"),
        )
        val hadoop = new HadoopPersonSortAndSerializer(c, h)
        timed(
          "hadoop person serialization",
          hadoop.run(fixturePath / "merged_persons")
        )
      }

      {
        val (h, c) = updateConf(hadoopConf,
          "serializer.buildDir" -> (conf.getBuildDir / "actual"),
          "serializer.socialNetworkDir" -> (conf.getSocialNetworkDir / "actual"),
        )
        timed(
          "spark person activity",
          SparkPersonSerializer(persons, c, Some(HadoopConfiguration.getNumThreads(h)))
        )
      }

      val datasets = Seq(
        "person",
        "person_knows_person",
        "person_hasInterest_tag",
      )

      forAll(datasets) { ds =>
        val expected = concatCsvFileRows((for {i <- 0 until numPartitions} yield s"${conf.getSocialNetworkDir}/expected/dynamic/${ds}_${i}_0.csv"): _*).toList
        val actual = concatCsvFileRows((for {i <- 0 until numPartitions} yield s"${conf.getSocialNetworkDir}/actual/dynamic/${ds}_${i}_0.csv"): _*).toList

        actual.length shouldBe expected.length

        actual.toSet shouldBe expected.toSet
      }
    }
  }

  def concatCsvFileRows(csvFiles: String*): Seq[String] = {
    for {
      csvFile <- csvFiles
      line <- Files.readAllLines(Paths.get(csvFile), charset).iterator.asScala.drop(1)
    } yield line
  }

  test("Person activity serializer generates & serializes the same activities") {
    withHadoopConf() { (conf, hadoopConf) =>
      val persons = spark.sparkContext
        .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](fixturePath / "merged_persons")
        .values

      val numPartitions = HadoopConfiguration.getNumThreads(hadoopConf)

      val ranker = SparkRanker.create(_.byRandomId)

      {
        val (h, c) = updateConf(hadoopConf,
          "serializer.buildDir" -> (conf.getBuildDir / "expected"),
          "serializer.socialNetworkDir" -> (conf.getSocialNetworkDir / "expected"),
        )
        val hadoop = new HadoopPersonActivityGenerator(c, h)
        timed(
          "hadoop person activity",
          hadoop.run(fixturePath / "merged_persons")
        )
      }

      {
        val (h, c) = updateConf(hadoopConf,
          "serializer.buildDir" -> (conf.getBuildDir / "actual"),
          "serializer.socialNetworkDir" -> (conf.getSocialNetworkDir / "actual"),
        )
        timed(
          "spark person activity",
          SparkActivitySerializer(persons, ranker, c, Some(HadoopConfiguration.getNumThreads(h)))
        )
      }

      val datasets = Seq(
        "comment",
        "forum",
        "post",
      )

      forAll(datasets) { ds =>
        val expected = concatCsvFileRows((for {i <- 0 until numPartitions} yield s"${conf.getSocialNetworkDir}/expected/dynamic/${ds}_${i}_0.csv"): _*).toList
        val actual = concatCsvFileRows((for {i <- 0 until numPartitions} yield s"${conf.getSocialNetworkDir}/actual/dynamic/${ds}_${i}_0.csv"): _*).toList

        actual.length shouldBe expected.length

        actual.toSet shouldBe expected.toSet
      }
    }
  }

  def updateConf(hadoopConf: Configuration, updates: (String, String)*): (Configuration, LdbcConfiguration) = {
    val cloned = new Configuration(hadoopConf)
    for {(k, v) <- updates} cloned.set(k, v)
    (cloned, HadoopConfiguration.extractLdbcConfig(cloned))
  }

  def shouldGenerateSameKnows[K: ClassTag : Ordering](
    hadoopFile: String,
    stepIndex: Int,
    sparkSorter: Person => K,
    hadoopSorter: String,
    conf: LdbcConfiguration,
    hadoopConf: Configuration,
    knowsGeneratorClassName: String = "ldbc.snb.datagen.generator.generators.knowsgenerators.DistanceKnowsGenerator"
  ) = {
    val personsFixturePath = fixturePath / "persons"

    val knowsGeneratorClassName = "ldbc.snb.datagen.generator.generators.knowsgenerators.DistanceKnowsGenerator"
    val preKeyHadoop = hadoopSorter
    val postKeySetter = "ldbc.snb.datagen.hadoop.miscjob.keychanger.RandomKeySetter"

    val percentages = Seq(0.45f, 0.45f, 0.1f)
    timed(
      s"hadoop knows generation for $hadoopFile",
      new HadoopKnowsGenerator(
        conf,
        hadoopConf,
        preKeyHadoop,
        postKeySetter,
        percentages.map(new lang.Float(_)).asJava,
        stepIndex,
        knowsGeneratorClassName
      ).run(personsFixturePath, conf.getBuildDir + hadoopFile)
    )

    val persons = spark.sparkContext
      .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](fixturePath / "persons")
      .values

    val ranker = SparkRanker.create(sparkSorter)

    val actual = SparkKnowsGenerator(persons, ranker, conf, percentages, stepIndex, knowsGeneratorClassName)

    val expected = spark.sparkContext
      .hadoopFile[TupleKey, Person, SequenceFileInputFormat[TupleKey, Person]](conf.getBuildDir + hadoopFile)

    val expecteds = expected.map { case (_, p) => p.hashCode() }.collect().toSet
    val actuals = actual.map(_.hashCode()).collect().toSet

    expecteds should have size 1700
    actuals shouldBe expecteds
  }

  def timed[A](name: String, thunk: => A): A = {
    val start = System.nanoTime()
    val res = thunk
    val end = System.nanoTime()
    println(s"$name: ${(end.toFloat - start) / 1e9f} s")
    res
  }
}

