package ldbc.snb.datagen.io

trait Reader[T] {
  type Ret

  def read(self: T): Ret
  def exists(self: T): Boolean
}

object Reader {
  type Aux[T, R] = Reader[T] { type Ret = R }

  def apply[T, R](implicit r: Reader.Aux[T, R]): Reader.Aux[T, R] = implicitly[Reader.Aux[T, R]]

  trait ReaderOps[T] {
    type Ret
    def tcInstance: Reader.Aux[T, Ret]
    def self: T
    def read: Ret       = tcInstance.read(self)
    def exists: Boolean = tcInstance.exists(self)
  }

  object ReaderOps {
    type Aux[T, R] = ReaderOps[T] { type Ret = R }
  }

  object ops {
    import scala.language.implicitConversions
    implicit def toReaderOps[T, R](target: T)(implicit tc: Reader.Aux[T, R]): ReaderOps.Aux[T, R] = new ReaderOps[T] {
      override type Ret = R
      override def tcInstance: Aux[T, R] = tc
      override def self: T               = target
    }
  }
}
