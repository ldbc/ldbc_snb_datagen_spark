package ldbc.snb.datagen.spark.util

object FluentSyntax {

  implicit class FoldSyntax[A](val self: A) extends AnyVal {
    /**
     * Fluent syntax for folding with the target as the base item.
     */
    def withFoldLeft[F](foldable: TraversableOnce[F], op: (A, F) => A): A = {
      foldable.foldLeft(self)(op)
    }
  }
}
