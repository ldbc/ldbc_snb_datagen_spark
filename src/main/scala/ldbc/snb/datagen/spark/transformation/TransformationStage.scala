package ldbc.snb.datagen.spark.transformation

import ldbc.snb.datagen.DatagenContext
import ldbc.snb.datagen.dictionary.Dictionaries
import ldbc.snb.datagen.spark.{LdbcDatagen, SparkApp}
import ldbc.snb.datagen.spark.transformation.model.Cardinality.{NN, OneN}
import ldbc.snb.datagen.spark.transformation.model.EntityType.{Edge, Node}
import ldbc.snb.datagen.spark.transformation.model.{GraphDef, Mode}
import ldbc.snb.datagen.spark.transformation.reader.DataFrameGraphReader
import ldbc.snb.datagen.spark.transformation.transform.{ExplodeAttrs, ExplodeEdges, RawToBiTransform, RawToInteractiveTransform}
import ldbc.snb.datagen.spark.transformation.writer.{BatchedGraphCsvWriter, GraphCsvWriter}
import ldbc.snb.datagen.syntax.FluentSyntaxForAny
import org.apache.spark.sql.SparkSession

object TransformationStage extends SparkApp {
  override def appName: String = "LDBC SNB Datagen for Spark: TransformationStage"

  case class Args(
    propFile: String,
    socialNetworkDir: String,
    outputDir: String,
    numThreads: Option[Int] = None
  )

  val inputGraphDefinition = GraphDef(
    "CompositeMergeForeign",
    Mode.Raw,
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

    val numPartitions = config.getInt("hadoop.numThreads", spark.sparkContext.defaultParallelism)

    DatagenContext.initialize(config)

    val reader = new DataFrameGraphReader()
    val bulkLoadThreshold = Dictionaries.dates.getBulkLoadThreshold
    val simulationEnd = Dictionaries.dates.getSimulationEnd
    val graph = reader
      .read(inputGraphDefinition, args.socialNetworkDir)
//      .let(graph => ExplodeEdges.transform(graph))
//      .let(graph => ExplodeAttrs.transform(graph))
      .let(graph => RawToBiTransform(bulkLoadThreshold, simulationEnd).transform(graph))


    BatchedGraphCsvWriter(header = true).write(graph, args.outputDir)
  }
}
