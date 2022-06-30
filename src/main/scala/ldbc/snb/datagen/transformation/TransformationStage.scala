package ldbc.snb.datagen.transformation

import ldbc.snb.datagen.io.graphs.{GraphSink, GraphSource}
import ldbc.snb.datagen.model
import ldbc.snb.datagen.model.{BatchedEntity, Graph, Mode}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.transformation.transform._
import ldbc.snb.datagen.util.{DatagenStage, Logging}
import org.apache.spark.sql.DataFrame
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
      irFormat: String = "parquet",
      format: String = "csv",
      formatOptions: Map[String, String] = Map.empty,
      epochMillis: Boolean = false
  )

  override type ArgsType = Args

  import ldbc.snb.datagen.io.Reader.ops._
  import ldbc.snb.datagen.io.Writer.ops._
  import ldbc.snb.datagen.io.instances._
  import ldbc.snb.datagen.transformation.transform.ConvertDates.instances._

  def run(args: ArgsType) = {
    object write extends Poly1 {
      implicit def caseSimple[M <: Mode](implicit ev: M#Layout =:= DataFrame) =
        at[Graph[M]](g => g.write(GraphSink(args.outputDir, args.format, args.formatOptions)))

      implicit def caseBatched[M <: Mode](implicit ev: M#Layout =:= BatchedEntity) =
        at[Graph[M]](g => g.write(GraphSink(args.outputDir, args.format, args.formatOptions)))
    }

    object convertDates extends Poly1 {
      implicit def caseSimple[M <: Mode](implicit ev: DataFrame =:= M#Layout) =
        at[Graph[M]](g => ConvertDates[M].transform(g))

      implicit def caseBatched[M <: Mode](implicit ev: BatchedEntity =:= M#Layout) =
        at[Graph[M]](g => ConvertDates[M].transform(g))
    }

    type Out = Graph[Mode.Raw.type] :+: Graph[Mode.Interactive] :+: Graph[Mode.BI] :+: CNil

    GraphSource(model.graphs.Raw.graphDef, args.outputDir, args.irFormat).read
      .pipeFoldLeft(args.explodeAttrs.fork)((graph, _: Unit) => ExplodeAttrs.transform(graph))
      .pipeFoldLeft(args.explodeEdges.fork)((graph, _: Unit) => ExplodeEdges.transform(graph))
      .pipe[Out] { g =>
        args.mode match {
          case bi @ Mode.BI(_, _) => Coproduct[Out](RawToBiTransform(bi, args.simulationStart, args.simulationEnd, args.keepImplicitDeletes).transform(g))
          case interactive @ Mode.Interactive(_) =>
            Coproduct[Out](RawToInteractiveTransform(interactive, args.simulationStart, args.simulationEnd).transform(g))
          case Mode.Raw => Coproduct[Out](g)
        }
      }
      .pipeFoldLeft((!args.epochMillis).fork)((graph, _: Unit) => graph.map(convertDates))
      .map(write)
    ()
  }
}
