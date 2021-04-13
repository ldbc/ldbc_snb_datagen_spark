package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.transformation.model.Mode
import ldbc.snb.datagen.transformation.model.Cardinality.{NN, NOne, OneN}
import ldbc.snb.datagen.transformation.model.EntityType.{Edge, Node}
import ldbc.snb.datagen.transformation.model.Mode.Raw.withRawColumns
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import shapeless.lens

object ExplodeEdges extends Transform[Mode.Raw.type, Mode.Raw.type]{
  override def transform(input: In): Out = {
    val entities = input.entities

    def explodedEdge(edge: Edge, node: DataFrame, column: Column) = {
      val Seq(src, dst) = edge.primaryKey

      edge.cardinality match {
        case OneN => edge -> node.select(withRawColumns(edge, $"id".as(src), explode(split(column, ";")).as(dst)))
        case NOne => edge -> node.select(withRawColumns(edge, explode(split(column, ";")).as(src), $"id".as(dst)))
        case NN => throw new IllegalArgumentException(s"Cannot explode edge with NN cardinality: $edge")
      }
    }

    val updatedEntities = entities.collect {
      case (k@Node("Organisation", true), v) => Map(
        explodedEdge(Edge("IsLocatedIn", "Organisation", "Place", OneN, isStatic = true), v, $"place"),
        k -> v.drop("place")
      )
      case (k@Node("Place", true), v) => Map(
        explodedEdge(Edge("IsPartOf", "Place", "Place", OneN, isStatic = true), v, $"isPartOf"),
        k -> v.drop("isPartOf")
      )
      case (k@Node("Tag", true), v) => Map(
        explodedEdge(Edge("HasType", "Tag", "TagClass", OneN, isStatic = true), v, $"hasType"),
        k -> v.drop("hasType")
      )
      case (k@Node("TagClass", true), v) => Map(
        explodedEdge(Edge("IsSubclassOf", "TagClass", "TagClass", OneN, isStatic = true), v, $"isSubclassOf"),
        k -> v.drop("isSubclassOf")
      )
      case (k@Node("Comment", false), v) => Map(
        explodedEdge(Edge("HasCreator", "Comment", "Person", OneN), v, $"creator"),
        explodedEdge(Edge("IsLocatedIn", "Comment", "Place", OneN), v, $"place"),
        explodedEdge(Edge("ReplyOf", "Comment", "Comment", OneN), v, $"replyOfComment"),
        explodedEdge(Edge("ReplyOf", "Comment", "Post", OneN), v, $"replyOfPost"),
        k -> v.drop("creator", "place", "replyOfPost", "replyOfComment")
      )
      case (k@Node("Forum", false), v) => Map(
        explodedEdge(Edge("HasModerator", "Forum", "Person", OneN), v, $"moderator"),
        k -> v.drop("moderator")
      )

      case (k@Node("Person", false), v) => Map(
        explodedEdge(Edge("IsLocatedIn", "Person", "Place", OneN), v, $"place"),
        k -> v.drop("place")
      )

      case (k@Node("Post", false), v) => Map(
        explodedEdge(Edge("HasCreator", "Post", "Person", OneN), v, $"creator"),
        explodedEdge(Edge("IsLocatedIn", "Post", "Place", OneN), v, $"place"),
        explodedEdge(Edge("ContainerOf", "Forum", "Post", NOne), v, $"`Forum.id`"),
        k -> v.drop("creator", "place", "`Forum.id`")
      )
    }.foldLeft(entities)(_ ++ _)

    val l = lens[In]
    (l.isEdgesExploded ~ l.entities).set(input)((true, updatedEntities))
  }
}
