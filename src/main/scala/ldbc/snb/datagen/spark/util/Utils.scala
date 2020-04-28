package ldbc.snb.datagen.spark.util

import java.io.IOException

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
}
