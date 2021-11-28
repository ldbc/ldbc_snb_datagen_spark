package ldbc.snb.datagen.io

import ldbc.snb.datagen.model.{GraphLike, Mode}

trait PathComponent[A] {
  def path(a: A): String
}

object PathComponent {
  def apply[A: PathComponent]: PathComponent[A] = implicitly[PathComponent[A]]

  private def pure[A](f: A => String) = new PathComponent[A] { override def path(a: A): String = f(a) }

  implicit def pathComponentForGraphDef[M <: Mode] = pure((g: GraphLike[M]) => {
    val explodedPart = g match {
      case g if g.isAttrExploded && g.isEdgesExploded => "singular-projected-fk"
      case g if g.isAttrExploded                      => "singular-merged-fk"
      case g if g.isEdgesExploded                     => "composite-projected-fk"
      case _                                          => "composite-merged-fk"
    }

    val modePart = g.mode.modePath

    s"$modePart/$explodedPart"
  })
}
