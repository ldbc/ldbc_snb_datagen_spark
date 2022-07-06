package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.model.EntityType.{Attr, Node}
import ldbc.snb.datagen.model.Mode
import ldbc.snb.datagen.model.Mode.Raw.withRawColumns
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.Logging
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql.{Column, DataFrame}
import shapeless.lens

object ExplodeAttrs extends Transform[Mode.Raw.type, Mode.Raw.type] with Logging {
  override def transform(input: In): Out = {
    log.info(s"Running $this")

    def explodedAttr(attr: Attr, node: DataFrame, column: Column) =
      attr -> node.select(withRawColumns(attr, $"id".as(s"${attr.parent}Id"), explode(split(column, ";")).as(s"${attr.attribute}Id")))

    val updatedEntities = input.entities
      .collect { case (k @ Node("Person", false), v) =>
        Map(
          explodedAttr(Attr("Email", k, "EmailAddress"), v, $"email"),
          explodedAttr(Attr("Speaks", k, "Language"), v, $"language"),
          k -> v.drop("email", "language")
        )
      }
      .foldLeft(input.entities)(_ ++ _)

    val l = lens[In]

    (l.isAttrExploded ~ l.entities).set(input)((true, updatedEntities))
  }
}
