package ldbc.snb.datagen.syntax

import scala.language.implicitConversions

trait FluentSyntax {
  @`inline` implicit final def fluentSyntaxOps[A](a: A) = new FluentSyntaxOps(a)
}

final class FluentSyntaxOps[A](private val self: A) extends AnyVal {
  /**
   * Fluent syntax for folding with self as the base item.
   */
  def pipeFoldLeft[F](foldable: TraversableOnce[F], op: (A, F) => A): A = {
    foldable.foldLeft(self)(op)
  }

  /**
   * Fluent syntax for applying a function on self. d
   */
  def pipe[R](f: A => R): R = f(self)


  /**
   * Fluent syntax for applying a side-effect on self.
   */
  def tap(f: A => Unit): A = {
    f(self)
    self
  }
}