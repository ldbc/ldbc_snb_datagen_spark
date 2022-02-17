package ldbc.snb.datagen.syntax

import java.io.Closeable

trait Usable[A] {
  def use[Ret](a: A, f: A => Ret): Ret
}

trait UsableInstances {
  trait UsableOps[A] {
    def tcInstance: Usable[A]
    def self: A
    def use[Ret](f: A => Ret): Ret
  }

  trait ops {
    import scala.language.implicitConversions
    implicit def toUsableOps[A: Usable](target: A): UsableOps[A] = new UsableOps[A] {
      override def tcInstance: Usable[A]      = implicitly[Usable[A]]
      override def self: A                    = target
      override def use[Ret](f: A => Ret): Ret = tcInstance.use(target, f)
    }
  }

  implicit def usableForClosable[A <: Closeable] = new Usable[A] {
    override def use[Ret](self: A, f: A => Ret): Ret = {
      val res = self
      try {
        f(res)
      } finally {
        self.close()
      }
    }
  }

  implicit def usableForAutoCloseable[A <: AutoCloseable] = new Usable[A] {
    override def use[Ret](self: A, f: A => Ret): Ret = {
      val res = self
      try {
        f(res)
      } finally {
        self.close()
      }
    }
  }
}

object UsableInstances extends UsableInstances

trait UseSyntax extends UsableInstances with UsableInstances.ops
