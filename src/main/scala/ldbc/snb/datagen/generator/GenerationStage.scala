package ldbc.snb.datagen.generator

import ldbc.snb.datagen.generator.generators.{SparkKnowsGenerator, SparkKnowsMerger, SparkPersonGenerator, SparkRanker}
import ldbc.snb.datagen.generator.serializers.RawSerializer
import ldbc.snb.datagen.io.raw.{Csv, Parquet, RawSink}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util._
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI
object GenerationStage extends DatagenStage with Logging {

  case class Args(
      scaleFactor: String = "1",
      numThreads: Option[Int] = None,
      params: Map[String, String] = Map.empty,
      paramFile: Option[String] = None,
      outputDir: String = "out",
      format: String = "parquet",
      oversizeFactor: Option[Double] = None
  )

  override type ArgsType = Args

  private val serializedEntitySizeRelativeToSerializedPersonSize = Map(
    "Post" -> 0.7517956288386042,
    "Forum_hasMember_Person"-> 0.27633174241458947,
    "Post_hasTag_Tag"-> 0.2301602419350709,
    "Comment_hasTag_Tag"-> 0.2318217197388683,
    "Person"-> 1.0,
    "Comment"-> 0.6908809054683169,
    "Person_hasInterest_Tag"-> 0.23243404370933324,
    "Person_workAt_Company"-> 0.253561011731804,
    "Forum"-> 0.4243395784673797,
    "Person_likes_Post"-> 0.2761393092186092,
    "Person_knows_Person"-> 0.307707613654668,
    "Forum_hasTag_Tag"-> 0.2288021830281894,
    "Person_studyAt_University"-> 0.25761355711785233,
    "Person_likes_Comment"-> 0.27757218453957555
  ).map { case (k, v) => k -> 1 / v }

  def run(args: Args) = {
    val config = buildConfig(args)
    DatagenContext.initialize(config)

    // Doesn't make sense to have more partitions than blocks
    val numPartitions = Math
      .min(
        Math.ceil(DatagenParams.numPersons.toDouble / DatagenParams.blockSize).toLong,
        config.getInt("hadoop.numThreads", spark.sparkContext.defaultParallelism)
      )
      .toInt

    val persons = SparkPersonGenerator(config, Some(numPartitions))

    val percentages             = Seq(0.45f, 0.45f, 0.1f)
    val knowsGeneratorClassName = DatagenParams.getKnowsGenerator

    import ldbc.snb.datagen.entities.Keys._

    val uniRanker      = SparkRanker.create(_.byUni)
    val interestRanker = SparkRanker.create(_.byInterest)
    val randomRanker   = SparkRanker.create(_.byRandomId)

    val uniKnows      = SparkKnowsGenerator(persons, uniRanker, config, percentages, 0, knowsGeneratorClassName)
    val interestKnows = SparkKnowsGenerator(persons, interestRanker, config, percentages, 1, knowsGeneratorClassName)
    val randomKnows   = SparkKnowsGenerator(persons, randomRanker, config, percentages, 2, knowsGeneratorClassName)

    val merged = SparkKnowsMerger(uniKnows, interestKnows, randomKnows).cache()

    val format = args.format match {
      case "csv"     => Csv
      case "parquet" => Parquet
      case a         => throw new IllegalArgumentException(s"Format `${a}` is not supported by the generator.")
    }

    SparkUI.job(simpleNameOf[RawSerializer], "serialize persons") {
      val rawSerializer = new RawSerializer(randomRanker)
      rawSerializer.write(
        merged,
        RawSink(format, Some(numPartitions), config, args.oversizeFactor,
          perEntityOversizeFactor = serializedEntitySizeRelativeToSerializedPersonSize)
      )
    }
  }

  def openPropFileStream(uri: URI) = {
    val fs = FileSystem.get(uri, spark.sparkContext.hadoopConfiguration)
    fs.open(new Path(uri.getPath))
  }

  def buildConfig(args: Args) = {
    val conf = new java.util.HashMap[String, String]
    conf.putAll(getClass.getResourceAsStream("/params_default.ini") use { ConfigParser.readConfig })

    for { paramsFile <- args.paramFile } conf.putAll(openPropFileStream(URI.create(paramsFile)) use { ConfigParser.readConfig })

    for { (k, v) <- args.params } conf.put(k, v)

    for { numThreads <- args.numThreads } conf.put("hadoop.numThreads", numThreads.toString)

    conf.putAll(ConfigParser.scaleFactorConf(args.scaleFactor))
    conf.put("generator.outputDir", args.outputDir)
    new GeneratorConfiguration(conf)
  }
}
