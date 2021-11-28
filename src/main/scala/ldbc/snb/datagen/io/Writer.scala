package ldbc.snb.datagen.io

trait Writer[S] {
  type CoRet
  def write(self: CoRet, sink: S): Unit
}

object Writer {
  type Aux[S, C] = Writer[S] { type CoRet = C }
  def apply[S, C](implicit r: Writer.Aux[S, C]): Writer.Aux[S, C] = implicitly[Writer.Aux[S, C]]

  trait WriterOps[CoRet] {
    type Sink
    def tcInstance: Writer.Aux[Sink, CoRet]
    def self: CoRet
    def write(sink: Sink): Unit = tcInstance.write(self, sink)
  }

  object WriterOps {
    type Aux[CoRet, S] = WriterOps[CoRet] { type Sink = S }
  }

  object ops {
    import scala.language.implicitConversions
    implicit def toWriterOps[CoRet, S](target: CoRet)(implicit tc: Writer.Aux[S, CoRet]): WriterOps.Aux[CoRet, S] = new WriterOps[CoRet] {
      override type Sink = S
      override def tcInstance: Aux[S, CoRet] = tc
      override def self: CoRet               = target
    }
  }
}
