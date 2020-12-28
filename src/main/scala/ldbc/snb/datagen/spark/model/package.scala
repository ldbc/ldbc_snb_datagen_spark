package ldbc.snb.datagen.spark

import ldbc.snb.datagen.model.Graph
import org.apache.spark.sql.DataFrame

package object model {
  type DataFrameGraph = Graph[DataFrame]
}
