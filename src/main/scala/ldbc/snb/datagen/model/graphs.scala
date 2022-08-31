package ldbc.snb.datagen.model

import ldbc.snb.datagen.model.raw.RawEntity

object graphs {
  import instances._

  object Raw {

    import raw.instances._

    val graphDef = GraphDef(
      isAttrExploded = false,
      isEdgesExploded = false,
      useTimestamp = false,
      Mode.Raw,
      UntypedEntities[RawEntity].value.map { case (k, v) => (k, Some(v.toDDL)) }
    )
  }
}
