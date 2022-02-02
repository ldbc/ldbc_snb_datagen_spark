package ldbc.snb.datagen.generator

import ldbc.snb.datagen.generator.generators.{SparkKnowsGenerator, SparkKnowsMerger, SparkPersonGenerator, SparkRanker}
import ldbc.snb.datagen.generator.serializers.RawSerializer
import ldbc.snb.datagen.io.raw.{Csv, Parquet, RawSink}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util._
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI
object GenerationStage extends DatagenStage with Logging {
  val optimalPersonsPerFile = 500000

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

  def run(args: Args) = {
    val config = buildConfig(args)
    DatagenContext.initialize(config)

    val numPartitions   = config.getInt("hadoop.numThreads", spark.sparkContext.defaultParallelism)
    val idealPartitions = DatagenParams.numPersons.toDouble / optimalPersonsPerFile

    val oversizeFactor = args.oversizeFactor.getOrElse(Math.max(numPartitions / idealPartitions, 1.0))

    val persons = SparkPersonGenerator(config)

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
      rawSerializer.write(merged, RawSink(format, Some(numPartitions), config, oversizeFactor))
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
