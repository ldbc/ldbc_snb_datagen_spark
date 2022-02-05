package ldbc.snb.datagen

import ldbc.snb.datagen.model.{GraphDef, Mode}
import org.apache.spark.sql.DataFrame

package object factors {
  sealed trait FactorKind { def path: String }
  case object Factor extends FactorKind { def path = "factors" }
  case object Params extends FactorKind { def path = "params"  }

  case class FactorTableDef[M <: Mode](
      name: String,
      kind: FactorKind,
      sourceDef: GraphDef[M]
  )

  case class FactorTable[M <: Mode](
      definition: FactorTableDef[M],
      data: DataFrame
  )
}
