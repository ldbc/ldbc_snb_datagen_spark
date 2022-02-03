package ldbc.snb.datagen.factors

import ldbc.snb.datagen.io.dataframes.{DataFrameSink, DataFrameSource}
import ldbc.snb.datagen.io.{PathComponent, Reader, Writer}
import ldbc.snb.datagen.model.{GraphDef, Mode}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

package object io {
  case class FactorTableSink(path: String, format: String = "csv", overwrite: Boolean = false)

  case class FactorTableSource[M <: Mode](definition: FactorTableDef[M], path: String, format: String = "csv")

  import ldbc.snb.datagen.io.Writer.ops._
  import ldbc.snb.datagen.io.dataframes.instances._

  private final class FactorTableWriter[M <: Mode] extends Writer[FactorTableSink] with Logging {
    override type Data = FactorTable[M]

    override def write(self: FactorTable[M], sink: FactorTableSink): Unit = {
      val p = (sink.path / "factors" / sink.format / PathComponent[GraphDef[M]].path(self.definition.sourceDef) / self.definition.name).toString
      val dfSink = if (sink.overwrite) {
        DataFrameSink(p, sink.format, mode = SaveMode.Overwrite)
      } else DataFrameSink(p, sink.format)
      self.data.coalesce(1).write(dfSink)
      log.info(s"Factor table ${self.definition.name} written")
    }
  }

  trait WriterInstances {
    implicit def factorTableWriter[M <: Mode]: Writer.Aux[FactorTableSink, FactorTable[M]] = new FactorTableWriter[M]
  }

  import ldbc.snb.datagen.io.Reader.ops._

  private final class FactorTableReader[M <: Mode](implicit spark: SparkSession) extends Reader[FactorTableSource[M]] with Logging {
    override type Ret = FactorTable[M]

    override def read(self: FactorTableSource[M]): Ret = {
      val p = (self.path / "factors" / self.format / PathComponent[GraphDef[M]].path(self.definition.sourceDef) / self.definition.name).toString

      log.info(s"Reading factor table ${self.definition.name}")
      val df = DataFrameSource(p, self.format, Map.empty, schema = None).read
      FactorTable(self.definition, df)
    }

    override def exists(self: FactorTableSource[M]): Boolean = {
      val p = (self.path / "factors" / self.format / PathComponent[GraphDef[M]].path(self.definition.sourceDef) / self.definition.name).toString
      DataFrameSource(p, self.format, Map.empty, schema = None).exists
    }
  }

  trait ReaderInstances {
    implicit def factorTableReader[M <: Mode](implicit spark: SparkSession): Reader.Aux[FactorTableSource[M], FactorTable[M]] = new FactorTableReader[M]
  }

  object instances extends WriterInstances with ReaderInstances
}
