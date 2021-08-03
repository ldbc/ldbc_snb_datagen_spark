package ldbc.snb.datagen

import com.google.common.base.CaseFormat

import java.io.IOException
import java.util.function.IntFunction
import scala.reflect.ClassTag
import scala.util.control.NonFatal

package object util {
  def tryOrThrowIOException[T](block: => T): T = {
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

  def simpleNameOf[T: ClassTag] = implicitly[ClassTag[T]].runtimeClass.getSimpleName

  def snake(str: String) = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, str)

  def camel(str: String) = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, str)

  def lower(str: String) = str.toLowerCase
}
