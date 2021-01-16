package ldbc.snb.datagen.spark.transformation

import ldbc.snb.datagen.spark.SparkApp
import ldbc.snb.datagen.spark.transformation.model.Cardinality.{NN, OneN}
import ldbc.snb.datagen.spark.transformation.model.EntityType.{Edge, Node}
import ldbc.snb.datagen.spark.transformation.model.{Graph, GraphDef}
import ldbc.snb.datagen.spark.transformation.reader.DataFrameGraphReader
import ldbc.snb.datagen.spark.transformation.writer.CsvWriter
import org.apache.spark.sql.SparkSession

object TransformationStage extends SparkApp {
  override def appName: String = "LDBC SNB Datagen for Spark: TransformationStage"

  case class Args(
    socialNetworkDir: String,
    outputDir: String,
    numThreads: Option[Int] = None
  )

  val inputGraphDefinition = GraphDef(
    "CompositeMergeForeignRaw",
    Set(
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

  def run(args: Args)(implicit spark: SparkSession) = {
    val reader = new DataFrameGraphReader()
    val graph = reader.read(inputGraphDefinition, args.socialNetworkDir)

    CsvWriter().write(graph, args.outputDir)
  }
}
