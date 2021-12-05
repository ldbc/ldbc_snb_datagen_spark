package ldbc.snb.datagen.model

import ldbc.snb.datagen.model.raw.RawEntity

object graphs {
  object Raw {

    import instances._
    import ldbc.snb.datagen.model.raw.instances._

    val graphDef = GraphDef(
      isAttrExploded = false,
      isEdgesExploded = false,
      Mode.Raw,
      UntypedEntities[RawEntity].value.map { case (k, v) => (k, Some(v.toDDL)) }
    )
  }
}
