package ldbc.snb.datagen.syntax

import java.io.Closeable

import scala.language.implicitConversions

// "try with resources"
trait UseSyntax {
  @`inline` implicit final def useSyntaxForClosable[A <: Closeable](a: A)         = new UseSyntaxForClosable(a)
  @`inline` implicit final def useSyntaxForAutoClosable[A <: AutoCloseable](a: A) = new UseSyntaxForAutoClosable(a)
}

final class UseSyntaxForClosable[A <: Closeable](private val self: A) extends AnyVal {
  def use[B](f: A => B): B = {
    val res = self
    try {
      f(res)
    } finally {
      self.close()
    }
  }
}

final class UseSyntaxForAutoClosable[A <: AutoCloseable](private val self: A) extends AnyVal {
  def use[B](f: A => B): B = {
    val res = self
    try {
      f(res)
    } finally {
      self.close()
    }
  }
}
