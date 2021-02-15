package ldbc.snb.datagen.generation

import ldbc.snb.datagen.{DatagenContext, DatagenParams, SparkApp}
import ldbc.snb.datagen.generation.generator.{SparkKnowsGenerator, SparkKnowsMerger, SparkPersonGenerator, SparkRanker}
import ldbc.snb.datagen.generation.serializer.{SparkActivitySerializer, SparkPersonSerializer, SparkStaticGraphSerializer}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.{ConfigParser, GeneratorConfiguration, SparkUI}
import ldbc.snb.datagen.util.Utils.simpleNameOf
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.net.URI

object GenerationStage extends SparkApp {
  override def appName: String = "LDBC SNB Datagen for Spark: Generation Stage"

  case class Args(
    scaleFactor: Int = 0,
    numThreads: Option[Int] = None,
    params: Map[String, String] = Map.empty,
    paramFile: Option[String] = None,
    outputDir: String = "out"
  )

  def run(config: GeneratorConfiguration)(implicit spark: SparkSession) = {
    val numPartitions = config.getInt("hadoop.numThreads", spark.sparkContext.defaultParallelism)

    val persons = SparkPersonGenerator(config)

    val percentages = Seq(0.45f, 0.45f, 0.1f)
    val knowsGeneratorClassName = DatagenParams.getKnowsGenerator

    import ldbc.snb.datagen.entities.Keys._

    val uniRanker = SparkRanker.create(_.byUni)
    val interestRanker = SparkRanker.create(_.byInterest)
    val randomRanker = SparkRanker.create(_.byRandomId)

    val uniKnows = SparkKnowsGenerator(persons, uniRanker, config, percentages, 0, knowsGeneratorClassName)
    val interestKnows = SparkKnowsGenerator(persons, interestRanker, config, percentages, 1, knowsGeneratorClassName)
    val randomKnows = SparkKnowsGenerator(persons, randomRanker, config, percentages, 2, knowsGeneratorClassName)

    val merged = SparkKnowsMerger(uniKnows, interestKnows, randomKnows).cache()

    SparkUI.job(simpleNameOf[SparkActivitySerializer.type], "serialize person activities") {
      SparkActivitySerializer(merged, randomRanker, config, Some(numPartitions))
    }

    SparkUI.job(simpleNameOf[SparkPersonSerializer.type ], "serialize persons") {
      SparkPersonSerializer(merged, config, Some(numPartitions))
    }

    SparkUI.job(simpleNameOf[SparkStaticGraphSerializer.type], "serialize static graph") {
      SparkStaticGraphSerializer(config, Some(numPartitions))
    }
  }

  def openPropFileStream(uri: URI) = {
    val fs = FileSystem.get(uri, spark.sparkContext.hadoopConfiguration)
    fs.open(new Path(uri.getPath))
  }

  def buildConfig(args: Args) = {
    val conf = ConfigParser.defaultConfiguration()

    conf.putAll(getClass.getResourceAsStream("/params_default.ini") use { ConfigParser.readConfig })

    for { paramsFile <- args.paramFile } conf.putAll(openPropFileStream(URI.create(paramsFile)) use { ConfigParser.readConfig })

    for { (k, v) <- args.params } conf.put(k, v)

    for { numThreads <- args.numThreads } conf.put("hadoop.numThreads", numThreads.toString)

    conf.put("generator.scaleFactor", args.scaleFactor.toString)

    new GeneratorConfiguration(conf)
  }
}
