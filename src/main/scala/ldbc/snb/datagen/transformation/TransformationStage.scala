package ldbc.snb.datagen.transformation

import ldbc.snb.datagen.io.graphs.{GraphSink, GraphSource}
import ldbc.snb.datagen.model
import ldbc.snb.datagen.model.{BatchedEntity, Graph, Mode}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.transformation.transform.{ExplodeAttrs, ExplodeEdges, ConvertDates, RawToBiTransform, RawToInteractiveTransform}
import ldbc.snb.datagen.util.{DatagenStage, Logging}
import org.apache.spark.sql.{DataFrame, SparkSession}
import shapeless._

object TransformationStage extends DatagenStage with Logging {
  case class Args(
      outputDir: String = "out",
      explodeEdges: Boolean = false,
      explodeAttrs: Boolean = false,
      keepImplicitDeletes: Boolean = false,
      simulationStart: Long = 0,
      simulationEnd: Long = 0,
      mode: Mode = Mode.Raw,
      format: String = "csv",
      formatOptions: Map[String, String] = Map.empty
  )

  import ldbc.snb.datagen.io.instances._
  import ldbc.snb.datagen.io.Writer.ops._
  import ldbc.snb.datagen.io.Reader.ops._

  def run(args: Args)(implicit spark: SparkSession) = {
    import spark.implicits._
    object write extends Poly1 {
      implicit def caseSimple[M <: Mode](implicit ev: M#Layout =:= DataFrame) =
        at[Graph[M]](g => g.write(GraphSink(args.outputDir, args.format, args.formatOptions)))

      implicit def caseBatched[M <: Mode](implicit ev: M#Layout =:= BatchedEntity) =
        at[Graph[M]](g => g.write(GraphSink(args.outputDir, args.format, args.formatOptions)))
    }

    type OutputTypes = Graph[Mode.Raw.type] :+:
      Graph[Mode.Interactive] :+:
      Graph[Mode.BI] :+:
      CNil

    GraphSource(model.graphs.Raw.graphDef, args.outputDir, "parquet").read
      .pipeFoldLeft(args.explodeAttrs.fork)((graph, _: Unit) => ExplodeAttrs.transform(graph))
      .pipeFoldLeft(args.explodeEdges.fork)((graph, _: Unit) => ExplodeEdges.transform(graph))
      .pipe(ConvertDates.transform)
      .pipe[OutputTypes] { g =>
        args.mode match {
          case bi @ Mode.BI(_, _) => Inr(Inr(Inl(RawToBiTransform(bi, args.simulationStart, args.simulationEnd, args.keepImplicitDeletes).transform(g))))
          case interactive @ Mode.Interactive(_) => Inr(Inl(RawToInteractiveTransform(interactive, args.simulationStart, args.simulationEnd).transform(g)))
          case Mode.Raw                          => Inl(g)
        }
      }
      .map(write)
    ()
  }
}
