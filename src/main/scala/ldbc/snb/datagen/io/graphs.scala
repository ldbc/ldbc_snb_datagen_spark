package ldbc.snb.datagen.io

import ldbc.snb.datagen.io.dataframes.{DataFrameSink, DataFrameSource}
import ldbc.snb.datagen.model.EntityType.{Attr, Edge, Node}
import ldbc.snb.datagen.model.Mode.Raw
import ldbc.snb.datagen.model._
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.{Logging, SparkUI}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import shapeless.{Generic, Poly1}

import scala.collection.immutable.TreeMap

object graphs {

  import Reader.ops._
  import Writer.ops._
  import dataframes.instances._

  case class GraphSink(
      path: String,
      format: String,
      formatOptions: Map[String, String] = Map.empty,
      saveMode: SaveMode = SaveMode.ErrorIfExists
  )

  private object CacheFriendlyEntityOrdering {
    // Heuristic for ordering entity types so that those derived
    // similarly are near each other.
    implicit val cacheFriendlyEntityOrdering = new Ordering[EntityType] {
      private val and = (a: Int, b: Int) => if (a == 0) b else a

      private object mapper extends Poly1 {
        implicit val atNode = at[Node](n => Seq(n.name.hashCode, 0, n.hashCode()))
        implicit val atEdge = at[Edge](e => {
          val primary = e match {
            case Edge("Likes", "Person", "Comment", _, _)   => "Comment"
            case Edge("Likes", "Person", "Post", _, _)      => "Post"
            case Edge("ContainerOf", "Forum", "Post", _, _) => "Post"
            case Edge(_, source, _, _, _)                   => source
          }
          Seq(primary.hashCode, 2, e.hashCode)
        })
        implicit val atAttr = at[Attr](a => Seq(a.parent.hashCode, 1, a.hashCode()))
      }

      private def orderingKeys(t: EntityType): Seq[Int] = Generic[EntityType].to(t).map(mapper).unify

      override def compare(x: EntityType, y: EntityType): Int =
        orderingKeys(x).zip(orderingKeys(y)).foldLeft(0)((a, v) => and(a, Ordering[Int].compare(v._1, v._2)))
    }
  }

  private trait GraphWriterMixin {
    protected def getFormatOptions(format: String, mode: Mode, customFormatOptions: Map[String, String] = Map.empty) = {
      val defaultCsvFormatOptions = Map(
        "header"          -> "true",
        "sep"             -> "|",
        "dateFormat"      -> Raw.datePattern,
        "timestampFormat" -> Raw.dateTimePattern,
        "nullValue"       -> null
      )

      val forcedRawCsvFormatOptions = Map(
        "dateFormat"      -> Raw.datePattern,
        "timestampFormat" -> Raw.dateTimePattern
      )

      val formatOptions: Map[String, String] = (format, mode) match {
        case ("csv", Raw) => defaultCsvFormatOptions ++ customFormatOptions ++ forcedRawCsvFormatOptions
        case ("csv", _)   => defaultCsvFormatOptions ++ customFormatOptions
        case _            => customFormatOptions
      }

      formatOptions
    }
  }

  private final class GraphWriter[M <: Mode](implicit
      the: M#Layout =:= DataFrame
  ) extends Writer[GraphSink]
      with Logging
      with GraphWriterMixin {

    override type Data = Graph[M]

    import CacheFriendlyEntityOrdering._

    override def write(self: Graph[M], sink: GraphSink): Unit = {
      TreeMap(self.entities.toSeq: _*).foreach { case (tpe, dataset) =>
        SparkUI.job(getClass.getSimpleName, s"write $tpe") {
          val p = (sink.path / "graphs" / sink.format / PathComponent[GraphLike[M]].path(self) / tpe.entityPath).toString
          log.info(s"$tpe: Writing started")
          val opts = getFormatOptions(sink.format, self.mode)
          the(dataset).write(DataFrameSink(p, sink.format, opts, SaveMode.Ignore))
          log.info(s"$tpe: Writing completed")
        }(dataset.sparkSession)
      }
    }
  }

  private final class BatchedGraphWriter[M <: Mode](implicit
      ev: M#Layout =:= BatchedEntity
  ) extends Writer[GraphSink]
      with Logging
      with GraphWriterMixin {

    override type Data = Graph[M]

    import CacheFriendlyEntityOrdering._

    override def write(self: Graph[M], sink: GraphSink): Unit = {
      val opts = getFormatOptions(sink.format, self.mode)
      TreeMap(self.entities.mapValues(ev).toSeq: _*).foreach { case (tpe, BatchedEntity(snapshot, insertBatches, deleteBatches)) =>
        SparkUI.job(getClass.getSimpleName, s"write $tpe snapshot") {
          val p = (sink.path / "graphs" / sink.format / PathComponent[GraphLike[M]].path(self) / "initial_snapshot" / tpe.entityPath).toString
          log.info(s"$tpe: Writing snapshot")
          snapshot.write(DataFrameSink(p, sink.format, opts, SaveMode.Ignore))
          log.info(s"$tpe: Writing snapshot completed")
        }(snapshot.sparkSession)

        for { (operation, batches) <- Map("inserts" -> insertBatches, "deletes" -> deleteBatches) } {
          batches.foreach { case Batched(entity, partitionKeys) =>
            SparkUI.job(getClass.getSimpleName, s"write $tpe $operation") {
              val p = (sink.path / "graphs" / sink.format / PathComponent[GraphLike[M]].path(self) / operation / tpe.entityPath).toString
              log.info(f"$tpe: Writing $operation")
              entity.write(DataFrameSink(p, sink.format, opts, SaveMode.Ignore, partitionBy = partitionKeys))
              log.info(f"$tpe: Writing $operation completed")
            }(entity.sparkSession)
          }
        }
      }
    }
  }

  trait WriterInstances {
    implicit def graphWriter[M <: Mode](implicit ev: M#Layout =:= DataFrame): Writer.Aux[GraphSink, Graph[M]] = new GraphWriter[M]

    implicit def batchedGraphWriter[M <: Mode](implicit ev: M#Layout =:= BatchedEntity): Writer.Aux[GraphSink, Graph[M]] = new BatchedGraphWriter[M]
  }

  case class GraphSource[M <: Mode](definition: GraphDef[M], path: String, format: String)

  private final class GraphReader[M <: Mode](implicit spark: SparkSession, ev: DataFrame =:= M#Layout) extends Reader[GraphSource[M]] with Logging {
    override type Ret = Graph[M]

    override def read(self: GraphSource[M]): Graph[M] = {
      val entities = for { (entity, schema) <- self.definition.entities } yield {
        val p = (self.path / "graphs" / self.format / PathComponent[GraphLike[M]].path(self.definition) / entity.entityPath).toString()
        log.info(s"Reading $entity")
        val opts = getFormatOptions(self.format, self.definition.mode)
        val df   = DataFrameSource(p, self.format, opts, schema.map(StructType.fromDDL)).read
        entity -> ev(df)
      }
      Graph[M](self.definition.isAttrExploded, self.definition.isEdgesExploded, self.definition.mode, entities)
    }

    override def exists(self: GraphSource[M]): Boolean = utils.fileExists(self.path)

    private def getFormatOptions(format: String, mode: Mode, customFormatOptions: Map[String, String] = Map.empty) = {
      val defaultCsvFormatOptions = Map(
        "header" -> "true",
        "sep"    -> "|"
      )

      val forcedRawCsvFormatOptions = Map(
        "dateFormat"      -> Raw.datePattern,
        "timestampFormat" -> Raw.dateTimePattern
      )

      val formatOptions: Map[String, String] = (format, mode) match {
        case ("csv", Raw) => defaultCsvFormatOptions ++ customFormatOptions ++ forcedRawCsvFormatOptions
        case ("csv", _)   => defaultCsvFormatOptions ++ customFormatOptions
        case _            => customFormatOptions
      }

      formatOptions
    }
  }

  trait ReaderInstances {
    implicit def graphReader[M <: Mode](implicit spark: SparkSession, ev: DataFrame =:= M#Layout): Reader.Aux[GraphSource[M], Graph[M]] =
      new GraphReader[M]
  }

  trait Instances extends WriterInstances with ReaderInstances

  object instances extends Instances

}
