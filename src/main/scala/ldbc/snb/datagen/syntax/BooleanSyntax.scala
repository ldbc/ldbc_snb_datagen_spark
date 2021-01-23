package ldbc.snb.datagen.syntax

import scala.language.implicitConversions

trait BooleanSyntax {
  @`inline` implicit final def booleanOps(a: Boolean) = new BooleanOps(a)
}

final class BooleanOps(private val bool: Boolean) extends AnyVal {
  def fork: Option[Unit] = if (bool) Some(()) else None
}
