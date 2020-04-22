package ldbc.snb.datagen.spark.entities

import ldbc.snb.datagen.entities.dynamic.person.Person
import org.apache.spark.sql.Encoder

object Encoders {

  implicit val personEncoder: Encoder[Person] = ???

}
