package ldbc.snb.datagen

import org.apache.spark.sql.ColumnName

object sql {
  def qcol(name: String) = new ColumnName(qualified(name))

  def qualified(col: String) = s"`$col`"
}
