package ldbc.snb.datagen.factors

import ldbc.snb.datagen.model.{Graph, GraphDef, Mode}
import org.apache.spark.sql.DataFrame


case class FactorTableDef[M <: Mode](
                            name: String,
                            sourceDef: GraphDef[M]
                            )

case class FactorTable[M <: Mode](
                        name: String,
                        data: DataFrame,
                        source: Graph[M]
                      )
