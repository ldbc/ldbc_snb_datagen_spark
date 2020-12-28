package ldbc.snb.datagen.spark.util

import com.google.common.base.CaseFormat
import org.apache.spark.sql.ColumnName

import java.io.{Closeable, IOException}
import java.util.function.IntFunction
import scala.reflect.ClassTag
import scala.util.control.NonFatal

object Utils {
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        throw e
      case NonFatal(e) =>
        throw new IOException(e)
    }
  }

  def arrayOfSize[A: ClassTag] = new IntFunction[Array[A]] {
    override def apply(value: Int) = new Array[A](value)
  }

  def snake(str: String) = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, str)

  def camel(str: String) = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, str)
}
