package ldbc.snb.datagen.spark.util

import java.io.{Closeable, IOException}

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

  // "try with resources"
  implicit class UseCloseable[A <: Closeable](val self: A) extends AnyVal {
    def use[B](f: A => B): B = {
      val res = self
      try {
        f(res)
      } finally {
        self.close()
      }
    }
  }
}
