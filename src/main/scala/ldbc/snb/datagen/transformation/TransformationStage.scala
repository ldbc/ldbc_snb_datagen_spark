package ldbc.snb.datagen.transformation

import ldbc.snb.datagen.dictionary.Dictionaries
import ldbc.snb.datagen.{DatagenContext, SparkApp}
import ldbc.snb.datagen.spark.LdbcDatagen
import ldbc.snb.datagen.transformation.model.Cardinality.{NN, OneN}
import ldbc.snb.datagen.transformation.model.EntityType.{Edge, Node}
import ldbc.snb.datagen.transformation.model.{BatchedEntity, Graph, GraphDef, Mode}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.transformation.io._
import ldbc.snb.datagen.transformation.model.Mode.Raw
import ldbc.snb.datagen.transformation.transform.{ExplodeAttrs, ExplodeEdges, RawToBiTransform, RawToInteractiveTransform}
import ldbc.snb.datagen.util.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import shapeless._

object TransformationStage extends SparkApp with Logging {
  override def appName: String = "LDBC SNB Datagen for Spark: TransformationStage"

  case class Args(
    propFile: String,
    socialNetworkDir: String,
    outputDir: String,
    numThreads: Option[Int] = None,
    explodeEdges: Boolean,
    explodeAttrs: Boolean,
    mode: Mode
  )

  val inputGraphDefinition = GraphDef[Mode.Raw.type](
    "CompositeMergeForeign",
    Set(
      Node("Organisation", isStatic = true),
      Node("Place", isStatic = true),
      Node("Tag", isStatic = true),
      Node("Tagclass", isStatic = true),
      Node("Comment"),
      Edge("HasTag", "Comment", "Tag", NN),
      Node("Forum"),
      Edge("HasMember", "Forum", "Person", NN),
      Edge("HasTag", "Forum", "Tag", NN),
      Node("Person"),
      Edge("HasInterest", "Person", "Tag", NN),
      Edge("Knows", "Person", "Person", NN),
      Edge("Likes", "Person", "Comment", NN),
      Edge("Likes", "Person", "Post", NN),
      Edge("StudyAt", "Person", "Organisation", OneN),
      Edge("WorkAt", "Person", "Organisation", NN),
      Node("Post"),
      Edge("HasTag", "Post", "Tag", NN)
    )
  )

  /*
  date formatting logic
  public class StringDateFormatter implements DateFormatter {
    private DateTimeFormatter gmtDateTimeFormatter;
    private DateTimeFormatter gmtDateFormatter;

    private static ZoneId GMT = ZoneId.of("GMT");

    public void initialize(LdbcConfiguration conf) {
        String formatDateTimeString = DatagenParams.getDateTimeFormat();
        gmtDateTimeFormatter = DateTimeFormatter.ofPattern(formatDateTimeString).withZone(GMT);
        String formatDateString = DatagenParams.getDateFormat();
        gmtDateFormatter = DateTimeFormatter.ofPattern(formatDateString).withZone(GMT);
    }

    public String formatDateTime(long date) {
        return gmtDateTimeFormatter.format(Instant.ofEpochMilli(date));
    }

    public String formatDate(long date) {
        return gmtDateFormatter.format(Instant.ofEpochMilli(date));
    }

}
   */

  def run(args: Args)(implicit spark: SparkSession) = {
    val config = LdbcDatagen.buildConfig(args.propFile, None, Some(args.socialNetworkDir), args.numThreads)

    val bulkLoadThreshold = Dictionaries.dates.getBulkLoadThreshold
    val simulationEnd = Dictionaries.dates.getSimulationEnd

    log.debug(s"bulkLoadThreshold: $bulkLoadThreshold")
    log.debug(s"simulationEnd: $simulationEnd")

    val numPartitions = config.getInt("hadoop.numThreads", spark.sparkContext.defaultParallelism)

    DatagenContext.initialize(config)

    object write extends Poly1 {
      implicit def caseSimple[M <: Mode](implicit ev: M#Layout[DataFrame] =:= DataFrame) = at[Graph[M, DataFrame]](
        GraphWriter[M, DataFrame].write(_, args.outputDir, WriterOptions())
      )
      implicit def caseBatched[M <: Mode](implicit ev: M#Layout[DataFrame] =:= BatchedEntity[DataFrame]) = at[Graph[M, DataFrame]](
        GraphWriter[M, DataFrame].write(_, args.outputDir, WriterOptions())
      )
    }

    type OutputTypes = Graph[Mode.Raw.type, DataFrame] :+:
      Graph[Mode.Interactive.type, DataFrame] :+:
      Graph[Mode.BI.type, DataFrame] :+:
      CNil

    GraphReader[Mode.Raw.type, DataFrame]
      .read(inputGraphDefinition, args.socialNetworkDir)
      .pipeFoldLeft(args.explodeAttrs.fork, (graph, _: Unit) => ExplodeAttrs.transform(graph))
      .pipeFoldLeft(args.explodeEdges.fork, (graph, _: Unit) => ExplodeEdges.transform(graph))
      .pipe[OutputTypes] {
        g => args.mode match {
          case Mode.BI => Inr(Inr(Inl(RawToBiTransform(bulkLoadThreshold, simulationEnd).transform(g))))
          case Mode.Interactive => Inr(Inl(RawToInteractiveTransform(bulkLoadThreshold, simulationEnd).transform(g)))
          case Mode.Raw => Inl(g)
        }
      }
      .pipe(_.map(write))
    ()
  }
}
