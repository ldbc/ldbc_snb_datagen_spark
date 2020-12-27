package ldbc.snb.datagen.spark

import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.Encoder

package object encoder extends EncoderInstances {
  implicit def encoderForTypedEncoder[T: TypedEncoder]: Encoder[T] = TypedExpressionEncoder[T]
}
