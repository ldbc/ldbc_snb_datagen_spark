package ldbc.snb.datagen.spark.util

import scala.io.Codec

trait SaneDefaults {
  implicit val codec = Codec.UTF8
  val charset = codec.charSet
}
