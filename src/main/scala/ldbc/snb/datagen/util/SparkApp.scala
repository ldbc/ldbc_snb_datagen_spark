package ldbc.snb.datagen.util

import ldbc.snb.datagen.syntax._
import org.apache.spark.sql.SparkSession

trait SparkApp {
  type ArgsType
  def appName: String

  def run(args: ArgsType): Unit

  implicit def spark: SparkSession = SparkSession
    .builder()
    .appName(appName)
    .pipe(applySparkConf(defaultSparkConf))
    .getOrCreate()

  private def applySparkConf(sparkConf: Map[String, String])(builder: SparkSession.Builder) =
    sparkConf.foldLeft(builder) { case (b, (k, v)) => b.config(k, v) }

  def defaultSparkConf: Map[String, String] = Map(
    "spark.sql.session.timeZone" -> "GMT"
  )

  protected lazy val env: SparkEnv = new SparkEnv
}

trait DatagenStage extends SparkApp {
  override val appName: String = s"LDBC SNB Datagen for Spark: ${this.getClass.getSimpleName.stripSuffix("$")}"
}
