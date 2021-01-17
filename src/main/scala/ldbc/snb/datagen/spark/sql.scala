package ldbc.snb.datagen.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, ColumnName, DataFrame, Dataset}

object sql {

  implicit class StringToColumn(val sc: StringContext) extends AnyVal {
    def $(args: Any*): ColumnName = new ColumnName(sc.s(args: _*))
  }

  def qcol(name: String) = new ColumnName(qualified(name))

  def qualified(col: String) = s"`$col`"

  implicit class DatasetExtensions[A](val self: Dataset[A]) extends AnyVal {
    def |+|(other: Dataset[A]): Dataset[A] = self union other

    def select(columns: Seq[Column]): DataFrame = self.select(columns: _*)

    def partition(expr: Column): (Dataset[A], Dataset[A]) = {
      val df = self.cache()
      (df.filter(expr), df.filter(!expr || expr.isNull))
    }
  }
}