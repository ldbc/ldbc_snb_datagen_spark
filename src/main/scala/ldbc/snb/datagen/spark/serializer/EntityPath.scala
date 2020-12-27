package ldbc.snb.datagen.spark.serializer

import ldbc.snb.datagen.model.EntityType
import ldbc.snb.datagen.model.EntityType.{Attr, Edge, Node}
import ldbc.snb.datagen.spark.util.Utils.camel
import shapeless._


trait EntityPath[T] {
  def entityPath(self: T): String
}

object EntityPath extends EntityPathInstances {
  def apply[A](implicit instance: EntityPath[A]): EntityPath[A] = instance
}

trait EntityPathInstances {
  private def nodePath(self: Node) = camel(self.name)
  private def edgePath(self: Edge) = s"${camel(self.source)}_${camel(self.`type`)}_${camel(self.destination)}"
  private def attrPath(self: Attr) = s"${camel(self.parent)}_${camel(self.`type`)}_${camel(self.attribute)}"

  private object entityTypePath extends Poly1 {
    implicit val atNode = at(nodePath)
    implicit val atEdge = at(edgePath)
    implicit val atAttr = at(attrPath)
  }

  implicit val entityPathForNode = new EntityPath[Node] {
    override def entityPath(self: Node) = nodePath(self)
  }

  implicit val entityPathForEdge = new EntityPath[Edge] {
    override def entityPath(self: Edge) = edgePath(self)
  }

  implicit val entityPathForAttr = new EntityPath[Attr] {
    override def entityPath(self: Attr) = attrPath(self)
  }

  implicit val entityPathForEntityType = new EntityPath[EntityType] {
    override def entityPath(self: EntityType) = Generic[EntityType].to(self).map(entityTypePath).unify
  }
}


