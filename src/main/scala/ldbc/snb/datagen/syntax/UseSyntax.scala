package ldbc.snb.datagen.syntax

import shapeless._

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

  implicit def usableForProduct[A <: Product, Repr](implicit gen: Generic.Aux[A, Repr], ev: Usable[Repr]): Usable[A] = new Usable[A] {
    override def use[Ret](a: A, f: A => Ret): Ret = {
      ev.use(gen.to(a), f.compose(gen.from))
    }
  }

  implicit def usableForHCons[A, Rest <: HList](implicit ev1: Usable[A], ev2: Usable[Rest]): Usable[A :: Rest] = new Usable[A :: Rest] {
    override def use[Ret](list: A :: Rest, f: (A :: Rest) => Ret): Ret = {
      ev1.use(list.head, (a: A) => { ev2.use(list.tail, (rest: Rest) => f(a :: rest)) })
    }
  }

  implicit val usableForHNil: Usable[HNil] = new Usable[HNil] {
    override def use[Ret](a: HNil, f: HNil => Ret): Ret = f(a)
  }
}

object UsableInstances extends UsableInstances

trait UseSyntax extends UsableInstances with UsableInstances.ops
