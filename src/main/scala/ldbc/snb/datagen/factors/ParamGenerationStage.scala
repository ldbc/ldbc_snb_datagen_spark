package ldbc.snb.datagen.factors

import ldbc.snb.datagen.factors.io.{FactorTableSink, FactorTableSource}
import ldbc.snb.datagen.model.graphs
import ldbc.snb.datagen.syntax.stringToColumnOps
import ldbc.snb.datagen.util.{DatagenStage, Logging}
import org.apache.spark.sql.DataFrame
import shapeless.lens

import scala.util.matching.Regex

trait ParamQueryTrait extends (Seq[DataFrame] => DataFrame) {
  override def apply(v1: Seq[DataFrame]): DataFrame
}

case class ParamQuery(requiredFactors: String*)(f: Seq[DataFrame] => DataFrame) extends ParamQueryTrait {
  override def apply(v1: Seq[DataFrame]): DataFrame = f(v1)
}

/*
case class ParamQuerySql(requiredFactors: String*)(f: Seq[String] => String)(implicit spark: SparkSession) extends ParamQueryTrait {
  override def apply(dfs: Seq[DataFrame]): DataFrame = {
    val registeredTableNames = for {
      (name, df) <- requiredFactors.zip(dfs)
      uniqueName = s"${name}_${UUID.randomUUID()}"
      _          = df.createOrReplaceTempView(uniqueName)
    } yield uniqueName
    try {
      spark.sql(f(registeredTableNames))
    } finally {
      for { t <- registeredTableNames } spark.sql(s"DROP TEMPORARY VIEW ${t}")
    }
  }
}
 */

object ParamGenerationStage extends DatagenStage with Logging {

  case class Args(
      outputDir: String = "out",
      only: Option[Regex] = None,
      force: Boolean = false
  )

  override type ArgsType = Args

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Args](getClass.getName.dropRight(1)) {
      head(appName)

      val args = lens[Args]

      opt[String]('o', "output-dir")
        .action((x, c) => args.outputDir.set(c)(x))
        .text(
          "path on the cluster filesystem, where Datagen outputs. Can be a URI (e.g S3, ADLS, HDFS) or a " +
            "path in which case the default cluster file system is used."
        )

      opt[String]("only")
        .action((x, c) => args.only.set(c)(Some(x.r.anchored)))
        .text("Only generate factor tables whose name matches the supplied regex")

      opt[Unit]("force")
        .action((_, c) => args.force.set(c)(true))
        .text("Overwrites existing output")

      help('h', "help").text("prints this usage text")
    }

    val parsedArgs = parser.parse(args, Args()).getOrElse(throw new RuntimeException("Invalid arguments"))

    run(parsedArgs)
  }

  def run(args: Args): Unit = {
    import ldbc.snb.datagen.factors.io.instances._
    import ldbc.snb.datagen.io.Reader.ops._
    import ldbc.snb.datagen.io.Writer.ops._

    val sourceDef = graphs.Raw.graphDef

    parameterQueries
      .collect { case (name, query) =>
        val tables =
          query.requiredFactors.map(factorTableName => FactorTableSource(FactorTableDef(factorTableName, Factor, sourceDef), args.outputDir).read.data)
        FactorTable(FactorTableDef(name, Params, sourceDef), query(tables))
      }
      .foreach { _.write(FactorTableSink(args.outputDir, overwrite = args.force)) }
  }

  private val parameterQueries = Map(
    "2" -> ParamQuery("tagClassNumMessages", "countryNumPersons") { case Seq(tnm, cnp) =>
      tnm.select($"TagClass.name").limit(10).crossJoin(cnp.select($"Country.name").limit(10))
    }
  )
}
