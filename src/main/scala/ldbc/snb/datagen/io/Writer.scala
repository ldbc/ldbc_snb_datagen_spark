package ldbc.snb.datagen.io

trait Writer[S] {
  type Data
  def write(self: Data, sink: S): Unit
}

object Writer {
  type Aux[S, D] = Writer[S] { type Data = D }
  def apply[S, D](implicit r: Writer.Aux[S, D]): Writer.Aux[S, D] = implicitly[Writer.Aux[S, D]]

  trait WriterOps[Data] {
    type Sink
    def tcInstance: Writer.Aux[Sink, Data]
    def self: Data
    def write(sink: Sink): Unit = tcInstance.write(self, sink)
  }

  object WriterOps {
    type Aux[Data, S] = WriterOps[Data] { type Sink = S }
  }

  object ops {
    import scala.language.implicitConversions
    implicit def toWriterOps[Data, S](target: Data)(implicit tc: Writer.Aux[S, Data]): WriterOps.Aux[Data, S] = new WriterOps[Data] {
      override type Sink = S
      override def tcInstance: Aux[S, Data] = tc
      override def self: Data               = target
    }
  }
}
