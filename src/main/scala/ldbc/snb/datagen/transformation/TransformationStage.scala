package ldbc.snb.datagen.transformation

import ldbc.snb.datagen.SparkApp
import ldbc.snb.datagen.transformation.model.Cardinality.{NN, OneN}
import ldbc.snb.datagen.transformation.model.EntityType.{Edge, Node}
import ldbc.snb.datagen.transformation.model.{BatchedEntity, Graph, GraphDef, Mode}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.transformation.io._
import ldbc.snb.datagen.transformation.transform.{ExplodeAttrs, ExplodeEdges, RawToBiTransform, RawToInteractiveTransform}
import ldbc.snb.datagen.util.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import shapeless._

object TransformationStage extends SparkApp with Logging {
  override def appName: String = "LDBC SNB Datagen for Spark: TransformationStage"

  case class Args(
    outputDir: String = "out",
    explodeEdges: Boolean = false,
    explodeAttrs: Boolean = false,
    simulationStart: Long = 0,
    simulationEnd: Long = 0,
    mode: Mode = Mode.Raw,
    format: String = "csv",
    formatOptions: Map[String, String] = Map.empty
  )

  val inputGraphDefinition = GraphDef(
    isAttrExploded = false,
    isEdgesExploded = false,
    Mode.Raw,
    Set(
      Node("Organisation", isStatic = true),
      Node("Place", isStatic = true),
      Node("Tag", isStatic = true),
      Node("TagClass", isStatic = true),
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
    object write extends Poly1 {
      implicit def caseSimple[M <: Mode](implicit ev: M#Layout[DataFrame] =:= DataFrame) = at[Graph[M, DataFrame]](g =>
        GraphWriter[M, DataFrame].write(g, args.outputDir, new FormatOptions(args.format, g.mode, args.formatOptions))
      )
      implicit def caseBatched[M <: Mode](implicit ev: M#Layout[DataFrame] =:= BatchedEntity[DataFrame]) = at[Graph[M, DataFrame]](g =>
        GraphWriter[M, DataFrame].write(g, args.outputDir, new FormatOptions(args.format, g.mode, args.formatOptions))
      )
    }

    type OutputTypes = Graph[Mode.Raw.type, DataFrame] :+:
      Graph[Mode.Interactive, DataFrame] :+:
      Graph[Mode.BI, DataFrame] :+:
      CNil

    GraphReader[Mode.Raw.type, DataFrame]
      .read(inputGraphDefinition, args.outputDir, new FormatOptions("csv", Mode.Raw))
      .pipeFoldLeft(args.explodeAttrs.fork)((graph, _: Unit) => ExplodeAttrs.transform(graph))
      .pipeFoldLeft(args.explodeEdges.fork)((graph, _: Unit) => ExplodeEdges.transform(graph))
      .pipe[OutputTypes] {
        g => args.mode match {
          case bi@Mode.BI(_, _) => Inr(Inr(Inl(RawToBiTransform(bi, args.simulationStart, args.simulationEnd).transform(g))))
          case interactive@Mode.Interactive(_) => Inr(Inl(RawToInteractiveTransform(interactive, args.simulationStart, args.simulationEnd).transform(g)))
          case Mode.Raw => Inl(g)
        }
      }
      .pipe(_.map(write))
    ()
  }
}
