package ldbc.snb.datagen.spark.transformation.transform

import ldbc.snb.datagen.spark.sql._
import ldbc.snb.datagen.spark.transformation.model
import ldbc.snb.datagen.spark.transformation.model.EntityType.{Attr, Node}
import ldbc.snb.datagen.spark.transformation.model.Graph
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{explode, split}

object ExplodeAttrs extends Transform.Uni[DataFrame] {
  override def transform(input: model.Graph[DataFrame]): model.Graph[DataFrame] = {

    def explodedAttr(attr: Attr, node: DataFrame, column: Column) =
      attr -> node.select($"id".as(s"${attr.parent}.id"), explode(split(column, ",")).as(s"${attr.attribute}.id"))

    val updatedEntities = input.entities.collect {
      case (k@Node("Person", false), v) => Map(
        explodedAttr(Attr("Email", "Person", "EmailAddress"), v, $"email"),
        explodedAttr(Attr("Speaks", "Person", "Language"), v, $"language"),
        k -> v.drop("email", "language")
      )
    }.foldLeft(input.entities)(_ ++ _)

    Graph("NonMerged", input.mode, updatedEntities)
  }
}
