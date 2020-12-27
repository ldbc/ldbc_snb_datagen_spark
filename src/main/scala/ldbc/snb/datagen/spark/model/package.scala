package ldbc.snb.datagen.spark

import org.apache.spark.sql.DataFrame

package object model {
  type DataFrameGraph = ldbc.snb.datagen.model.Graph[DataFrame]
}
