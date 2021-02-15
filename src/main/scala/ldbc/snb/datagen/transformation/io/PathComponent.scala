package ldbc.snb.datagen.transformation.io

import ldbc.snb.datagen.transformation.model.{GraphLike, Mode}

trait PathComponent[A] {
  def path(a: A): String
}

object PathComponent {
  def apply[A: PathComponent]: PathComponent[A] = implicitly[PathComponent[A]]

  private def pure[A](f: A => String) = new PathComponent[A] { override def path(a: A): String = f(a) }

  implicit def pathComponentForGraphDef[M <: Mode] = pure((g: GraphLike[M]) => {
    val explodedPart = g match {
      case g if g.isAttrExploded && g.isEdgesExploded => "basic"
      case g if g.isAttrExploded => "merge_foreign"
      case g if g.isEdgesExploded => "composite"
      case _ => "composite_merge_foreign"
    }

    val modePart = g.mode.modePath

    s"$modePart/$explodedPart"
  })
}
