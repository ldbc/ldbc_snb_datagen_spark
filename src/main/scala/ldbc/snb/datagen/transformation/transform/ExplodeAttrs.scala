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
    if (input.definition.isAttrExploded) {
      throw new IllegalArgumentException("Attributes already exploded in the input graph")
    }

    def explodedAttr(attr: Attr, node: DataFrame, column: Column) =
      attr -> node.select(withRawColumns(attr, $"id".as(s"${attr.parent}Id"), explode(split(column, ";")).as(s"${attr.attribute}Id")))

    val modifiedEntities = input.entities
      .collect { case (k @ Node("Person", false), v) =>
        Map(
          explodedAttr(Attr("Email", k, "EmailAddress"), v, $"email"),
          explodedAttr(Attr("Speaks", k, "Language"), v, $"language"),
          k -> v.drop("email", "language")
        )
      }

    val updatedEntities = modifiedEntities
      .foldLeft(input.entities)(_ ++ _)

    val updatedEntityDefinitions = modifiedEntities
      .foldLeft(input.definition.entities) { (e, v) =>
        e ++ v.map{ case (k, v) => k -> Some(v.schema.toDDL) }
      }

    val l = lens[In]

    (l.definition.isAttrExploded ~ l.definition.entities ~ l.entities).set(input)((true, updatedEntityDefinitions, updatedEntities))
  }
}
