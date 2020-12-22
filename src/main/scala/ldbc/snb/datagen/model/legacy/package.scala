package ldbc.snb.datagen.model

package object legacy {
  case class Person(
    isExplicitlyDeleted: Boolean,
    isMessageDeleter: Boolean,
    accountId: Long,
    creationDate: Long,
    deletionDate: Long,
    maxNumKnows: Long,
    knows: Set[Knows],
    browserId: Int,
    ipAddress: IP,
    countryId: Int,
    cityId: Int,
    interests: Set[Int],
    mainInterest: Int,
    universityLocationId: Int,
    gender: Byte,
    birthday: Long,
    isLargePoster: Boolean,
    randomId: Long,
    emails: Set[String],
    languages: List[Int],
    firstName: String,
    lastName: String,
    companies: Map[Long, Long],
    classYear: Long
  )

  case class IP(ip: Int, mask: Int)

  case class Knows(
    isExplicitlyDeleted: Boolean,
    to: PersonSummary,
    creationDate: Long,
    deletionDate: Long,
    weight: Float
  )

  case class PersonSummary(
    accountId: Long,
    creationDate: Long,
    deletionDate: Long,
    browserId: Int,
    country: Int,
    ipAddress: IP,
    isLargePoster: Boolean,
    isMessageDeleter: Boolean
  )
}
