package ldbc.snb.datagen.spark.transformation.transform

import ldbc.snb.datagen.spark.sql._
import ldbc.snb.datagen.spark.transformation.model
import ldbc.snb.datagen.spark.transformation.model.Graph
import ldbc.snb.datagen.spark.transformation.model.Cardinality.{NN, NOne, OneN}
import ldbc.snb.datagen.spark.transformation.model.EntityType.{Edge, Node}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object ExplodeEdges extends Transform.Uni[DataFrame]{
  override def transform(input: model.Graph[DataFrame]): model.Graph[DataFrame] = {

    val entities = input.entities

    def explodedEdge(edge: Edge, node: DataFrame, column: Column) = {
      val Seq(src, dst) = edge.primaryKey

      edge.cardinality match {
        case OneN => edge -> node.select($"id".as(src), explode(split(column, ",")).as(dst))
        case NOne => edge -> node.select(explode(split(column, ",")).as(src), $"id".as(dst))
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

    Graph(layout="NonComposite", mode=input.mode, entities=updatedEntities)
  }
}
