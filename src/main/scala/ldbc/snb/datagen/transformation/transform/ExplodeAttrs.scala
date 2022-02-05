package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.model.EntityType.{Attr, Node}
import ldbc.snb.datagen.model.Mode
import ldbc.snb.datagen.model.Mode.Raw.withRawColumns
import ldbc.snb.datagen.syntax._
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql.{Column, DataFrame}
import shapeless.lens

object ExplodeAttrs extends Transform[Mode.Raw.type, Mode.Raw.type] {
  override def transform(input: In): Out = {
    def explodedAttr(attr: Attr, node: DataFrame, column: Column) =
      attr -> node.select(withRawColumns(attr, $"id".as(s"${attr.parent}Id"), explode(split(column, ";")).as(s"${attr.attribute}Id")))

    val updatedEntities = input.entities
      .collect { case (k @ Node("Person", false), v) =>
        Map(
          explodedAttr(Attr("Email", "Person", "EmailAddress"), v, $"email"),
          explodedAttr(Attr("Speaks", "Person", "Language"), v, $"language"),
          k -> v.drop("email", "language")
        )
      }
      .foldLeft(input.entities)(_ ++ _)

    val l = lens[In]

    (l.definition.isAttrExploded ~ l.definition.entities ~ l.entities).set(input)(
      (true, updatedEntities.map { case (k, v) => (k, v.schema.some) }, updatedEntities)
    )
  }
}
