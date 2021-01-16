package ldbc.snb.datagen.spark

import better.files._
import ldbc.snb.datagen.spark.generation.GenerationStage
import ldbc.snb.datagen.spark.transformation.TransformationStage
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.net.URI

object LdbcDatagen extends SparkApp {
  val appName = "LDBC SNB Datagen for Spark"


  def openPropFileStream(uri: URI)(implicit spark: SparkSession) = {
    val fs = FileSystem.get(uri, spark.sparkContext.hadoopConfiguration)
    fs.open(new Path(uri.getPath))
  }


  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[GenerationStage.Args](getClass.getName.dropRight(1)) {
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

    val parsedArgs = parser.parse(args, GenerationStage.Args()).getOrElse(throw new RuntimeException("Invalid args"))

    GenerationStage.run(parsedArgs)

    TransformationStage.run(TransformationStage.Args(
      parsedArgs.socialNetworkDir.get,
      (parsedArgs.socialNetworkDir.get / "serialized" ).toString,
      parsedArgs.numThreads
    ))
  }
}



