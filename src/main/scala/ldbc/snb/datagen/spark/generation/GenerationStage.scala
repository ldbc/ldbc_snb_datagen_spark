package ldbc.snb.datagen.spark.generation

import ldbc.snb.datagen.{DatagenContext, DatagenParams}
import ldbc.snb.datagen.spark.{LdbcDatagen, SparkApp}
import ldbc.snb.datagen.spark.generation.generator.{SparkKnowsGenerator, SparkKnowsMerger, SparkPersonGenerator, SparkRanker}
import ldbc.snb.datagen.spark.generation.serializer.{SparkActivitySerializer, SparkPersonSerializer, SparkStaticGraphSerializer}
import ldbc.snb.datagen.spark.util.SparkUI
import ldbc.snb.datagen.spark.util.Utils.simpleNameOf
import org.apache.spark.sql.SparkSession

import java.net.URI

object GenerationStage extends SparkApp {
  override def appName: String = "LDBC SNB Datagen for Spark: Generation Stage"

  case class Args(
    propFile: String = "",
    buildDir: Option[String] = None,
    socialNetworkDir: Option[String] = None,
    numThreads: Option[Int] = None
  )

  def run(args: Args)(implicit spark: SparkSession) = {
    val config = LdbcDatagen.buildConfig(args.propFile, args.buildDir, args.socialNetworkDir, args.numThreads)

    val numPartitions = config.getInt("hadoop.numThreads", spark.sparkContext.defaultParallelism)

    DatagenContext.initialize(config)

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

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis

    val parser = new scopt.OptionParser[Args](getClass.getName.dropRight(1)) {
      head(appName)

      opt[String]("build-dir")
        .action((x, c) => c.copy(buildDir = Some(x)))
        .text("build directory for intermediate files")

      opt[String]("sn-dir")
        .action((x, c) => c.copy(socialNetworkDir = Some(x)))
        .text("output directory")

      opt[Int]("num-threads")
        .action((x, c) => c.copy(numThreads = Some(x)))
        .text("number of threads")


      help("help").text("prints this usage text")

      arg[String]("<param_file>").required()
        .action((x, c) => c.copy(propFile = x))
        .text("parameter file")
    }

    val parsedArgs = parser.parse(args, Args()).getOrElse(throw new RuntimeException("Invalid args"))

    run(parsedArgs)

    print("Total Execution time: " + ((System.currentTimeMillis - start) / 1000))
  }

}
