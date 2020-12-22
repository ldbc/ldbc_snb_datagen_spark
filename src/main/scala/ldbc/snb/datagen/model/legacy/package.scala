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

  case class ForumMembership(
    isExplicitlyDeleted: Boolean,
    forumId: Long,
    creationDate: Long,
    deletionDate: Long,
    person: PersonSummary,
    forumType: Forum.Type.Value
  )

  case class Forum(
    isExplicitlyDeleted: Boolean,
    id: Long,
    moderator: PersonSummary,
    moderatorDeletionDate: Long,
    creationDate: Long,
    deletionDate: Long,
    title: String,
    tags: List[Int],
    placeId: Int,
    language: Int,
    memberships: List[ForumMembership],
    forumType: Forum.Type.Value
  )

  object Forum {

    object Type extends Enumeration {
      type Type = Value
      val Wall, Album, Group = Type
    }

  }

  case class Like(
    isExplicitlyDeleted: Boolean,
    person: Long,
    personCreationDate: Long,
    messageId: Long,
    creationDate: Long,
    deletionDate: Long,
    `type`: Like.Type.Value
  )

  object Like {
    object Type extends Enumeration {
      type Type = Value
      val Post, Comment, Photo = Type
    }
  }

  case class Comment(
    isExplicitlyDeleted: Boolean,
    messageId: Long,
    creationDate: Long,
    deletionDate: Long,
    author: PersonSummary,
    forumId: Long,
    content: String,
    tags: Set[Int],
    ipAddress: IP,
    browserId: Int,
    countryId: Int,
    rootPostId: Long,
    parentMessageId: Long
  )

  case class Post(
    isExplicitlyDeleted: Boolean,
    messageId: Long,
    creationDate: Long,
    deletionDate: Long,
    author: PersonSummary,
    forumId: Long,
    content: String,
    tags: Set[Int],
    ipAddress: IP,
    browserId: Int,
    countryId: Int,
    language: Int
  )

  case class Photo(
    isExplicitlyDeleted: Boolean,
    messageId: Long,
    creationDate: Long,
    deletionDate: Long,
    author: PersonSummary,
    forumId: Long,
    content: String,
    tags: Set[Int],
    ipAddress: IP,
    browserId: Int,
    countryId: Int
  )


  type Wall[A] = List[(Forum, List[ForumMembership], List[A])]
  type PostTree = (Post, List[Like], List[(Comment, List[Like])])
  type PhotoTree = (Photo, List[Like])


  case class Activity(
    wall: Wall[PostTree],
    groups: List[Wall[PostTree]],
    albums: Wall[PhotoTree]
  )
}
