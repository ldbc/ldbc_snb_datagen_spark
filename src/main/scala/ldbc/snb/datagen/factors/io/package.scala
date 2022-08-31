package ldbc.snb.datagen.factors

import ldbc.snb.datagen.io.dataframes.DataFrameSink
import ldbc.snb.datagen.io.{PathComponent, Writer}
import ldbc.snb.datagen.model.{GraphDef, Mode}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.Logging
import org.apache.spark.sql.SaveMode

package object io {
  case class FactorTableSink(path: String, format: String = "parquet", overwrite: Boolean = false)

  import ldbc.snb.datagen.io.Writer.ops._
  import ldbc.snb.datagen.io.dataframes.instances._

  private final class FactorTableWriter[M <: Mode] extends Writer[FactorTableSink] with Logging {
    override type Data = FactorTable[M]

    override def write(self: FactorTable[M], sink: FactorTableSink): Unit = {
      val p = (sink.path / "factors" / sink.format / PathComponent[GraphDef[M]].path(self.source.definition) / self.name).toString
      val dfSink = if (sink.overwrite) {
        DataFrameSink(p, sink.format, mode = SaveMode.Overwrite)
      } else DataFrameSink(p, sink.format)
      self.data.write(dfSink)
      log.info(s"Factor table ${self.name} written")
    }
  }

  trait WriterInstances {
    implicit def factorTableWriter[M <: Mode]: Writer.Aux[FactorTableSink, FactorTable[M]] = new FactorTableWriter[M]
  }

  object instances extends WriterInstances
}
