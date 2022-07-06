package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.model.Cardinality._
import ldbc.snb.datagen.model.EntityType._
import ldbc.snb.datagen.model.Mode
import ldbc.snb.datagen.model.Mode.Raw.withRawColumns
import ldbc.snb.datagen.model.raw._
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import shapeless.lens

object ExplodeEdges extends Transform[Mode.Raw.type, Mode.Raw.type] with Logging {
  override def transform(input: In): Out = {
    log.info(s"Running $this")
    val entities = input.entities

    def explodedEdge(edge: Edge, node: DataFrame, column: Column) = {
      val Seq(src, dst) = edge.primaryKey

      edge.cardinality match {
        case OneN => edge -> node.select(withRawColumns(edge, $"id".as(src), explode(split(column, ";")).as(dst)))
        case NOne => edge -> node.select(withRawColumns(edge, explode(split(column, ";")).as(src), $"id".as(dst)))
        case NN   => throw new IllegalArgumentException(s"Cannot explode edge with NN cardinality: $edge")
      }
    }

    val updatedEntities = entities
      .collect {
        case (k @ OrganisationType, v) =>
          Map(
            explodedEdge(Edge("IsLocatedIn", k, PlaceType, OneN, isStatic = true), v, $"LocationPlaceId"),
            k -> v.drop("LocationPlaceId")
          )
        case (k @ PlaceType, v) =>
          Map(
            explodedEdge(Edge("IsPartOf", k, k, OneN, isStatic = true), v, $"PartOfPlaceId"),
            k -> v.drop("PartOfPlaceId")
          )
        case (k @ TagType, v) =>
          Map(
            explodedEdge(Edge("HasType", k, TagClassType, OneN, isStatic = true), v, $"TypeTagClassId"),
            k -> v.drop("TypeTagClassId")
          )
        case (k @ TagClassType, v) =>
          Map(
            explodedEdge(Edge("IsSubclassOf", k, k, OneN, isStatic = true), v, $"SubclassOfTagClassId"),
            k -> v.drop("SubclassOfTagClassId")
          )
        case (k @ CommentType, v) =>
          Map(
            explodedEdge(Edge("HasCreator", k, PersonType, OneN), v, $"CreatorPersonId"),
            explodedEdge(Edge("IsLocatedIn", k, PlaceType, OneN, destinationNameOverride = Some("Country")), v, $"LocationCountryId"),
            explodedEdge(Edge("ReplyOf", k, PostType, OneN), v, $"ParentPostId"),
            explodedEdge(Edge("ReplyOf", k, k, OneN), v, $"ParentCommentId"),
            k -> v.drop("CreatorPersonId", "LocationCountryId", "ParentPostId", "ParentCommentId")
          )
        case (k @ ForumType, v) =>
          Map(
            explodedEdge(Edge("HasModerator", k, PersonType, OneN), v, $"ModeratorPersonId"),
            k -> v.drop("ModeratorPersonId")
          )

        case (k @ PersonType, v) =>
          Map(
            explodedEdge(Edge("IsLocatedIn", k, PlaceType, OneN, destinationNameOverride = Some("City")), v, $"LocationCityId"),
            k -> v.drop("LocationCityId")
          )

        case (k @ PostType, v) =>
          Map(
            explodedEdge(Edge("HasCreator", k, PersonType, OneN), v, $"CreatorPersonId"),
            explodedEdge(Edge("ContainerOf", ForumType, k, NOne), v, $"ContainerForumId"),
            explodedEdge(Edge("IsLocatedIn", k, PlaceType, OneN, destinationNameOverride = Some("Country")), v, $"LocationCountryId"),
            k -> v.drop("CreatorPersonId", "LocationCountryId", "ContainerForumId")
          )
      }
      .foldLeft(entities)(_ ++ _)

    val l = lens[In]
    (l.isEdgesExploded ~ l.entities).set(input)((true, updatedEntities))
  }
}
