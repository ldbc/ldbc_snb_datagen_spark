package ldbc.snb.datagen.factors

import ldbc.snb.datagen.SparkApp
import ldbc.snb.datagen.factors.Factors.countryNumPersons
import ldbc.snb.datagen.factors.io.FactorTableSink
import ldbc.snb.datagen.io.graphs.GraphSource
import ldbc.snb.datagen.model
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.Logging
import org.apache.spark.sql.SparkSession

object FactorGenerationStage extends SparkApp with Logging {
  override def appName: String = "LDBC SNB Datagen for Spark: Factor Generation Stage"

  case class Args(outputDir: String = "out")

  def run(args: Args)(implicit spark: SparkSession): Unit = {
    import ldbc.snb.datagen.io.instances._
    import ldbc.snb.datagen.io.Reader.ops._
    import ldbc.snb.datagen.io.Writer.ops._
    import ldbc.snb.datagen.factors.io.instances._

    GraphSource(model.graphs.Raw.graphDef, args.outputDir, "csv")
      .read
      .pipe(countryNumPersons)
      .write(FactorTableSink(args.outputDir))
  }

}
