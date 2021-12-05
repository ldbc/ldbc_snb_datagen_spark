package ldbc.snb.datagen.factors

import ldbc.snb.datagen.io.{PathComponent, Writer}
import ldbc.snb.datagen.util.Logging
import ldbc.snb.datagen.model.{GraphLike, Mode}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.io.dataframes.DataFrameSink

package object io {
  case class FactorTableSink(path: String, format: String = "csv")

  import ldbc.snb.datagen.io.dataframes.instances._
  import ldbc.snb.datagen.io.Writer.ops._

  private final class FactorTableWriter[M <: Mode] extends Writer[FactorTableSink] with Logging {
    override type Data = FactorTable[M]

    override def write(self: FactorTable[M], sink: FactorTableSink): Unit = {
      val p = (sink.path / "factors" / sink.format / PathComponent[GraphLike[M]].path(self.source) / self.name).toString
      self.data.coalesce(1).write(DataFrameSink(p, sink.format))
      log.info(s"Factor table ${self.name} written")
    }
  }

  trait WriterInstances {
    implicit def factorTableWriter[M <: Mode]: Writer.Aux[FactorTableSink, FactorTable[M]] = new FactorTableWriter[M]
  }

  object instances extends WriterInstances
}
