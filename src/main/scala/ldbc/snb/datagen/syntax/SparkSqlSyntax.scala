package ldbc.snb.datagen.syntax

import org.apache.spark.sql.{Column, ColumnName, DataFrame, Dataset}

import scala.language.implicitConversions

trait SparkSqlSyntax {
  @`inline` implicit final def datasetOps[A](a: Dataset[A])           = new DatasetOps(a)
  @`inline` implicit final def stringToColumnOps[A](a: StringContext) = new StringToColumnOps(a)
}

final class DatasetOps[A](private val self: Dataset[A]) extends AnyVal {
  def |+|(other: Dataset[A]): Dataset[A] = self union other

  def select(columns: Seq[Column]): DataFrame = self.select(columns: _*)

  def partition(expr: Column): (Dataset[A], Dataset[A]) = {
    val df = self.cache()
    (df.filter(expr), df.filter(!expr || expr.isNull))
  }
}

final class StringToColumnOps(private val sc: StringContext) extends AnyVal {
  def $(args: Any*): ColumnName = new ColumnName(sc.s(args: _*))
}
