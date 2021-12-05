package ldbc.snb.datagen.model

import ldbc.snb.datagen.model.Cardinality.{NN, OneN}
import ldbc.snb.datagen.model.EntityType.{Edge, Node}

object raw {

  sealed trait RawEntity

  case class Person(
      creationDate: Long,
      deletionDate: Long,
      explicitlyDeleted: Boolean,
      id: Long,
      firstName: String,
      lastName: String,
      `gender`: String,
      `birthday`: Long,
      `locationIP`: String,
      `browserUsed`: String,
      `place`: Integer,
      `language`: String,
      `email`: String
  ) extends RawEntity

  case class PersonHasInterestTag(
      creationDate: Long,
      deletionDate: Long,
      personId: Long,
      interestIdx: Int
  ) extends RawEntity

  case class PersonKnowsPerson(
      creationDate: Long,
      deletionDate: Long,
      explicitlyDeleted: Boolean,
      `Person1Id`: Long,
      `Person2Id`: Long
  ) extends RawEntity

  case class PersonStudyAtUniversity(
      creationDate: Long,
      deletionDate: Long,
      explicitlyDeleted: Boolean,
      PersonId: Long,
      UniversityId: Long,
      classYear: Int
  ) extends RawEntity

  case class PersonWorkAtCompany(
      creationDate: Long,
      deletionDate: Long,
      explicitlyDeleted: Boolean,
      PersonId: Long,
      CompanyId: Long,
      workFrom: Int
  ) extends RawEntity

  case class Forum(
      creationDate: Long,
      deletionDate: Long,
      explicitlyDeleted: Boolean,
      id: Long,
      title: String,
      `ModeratorPersonId`: Long
  ) extends RawEntity

  case class ForumHasMember(
      creationDate: Long,
      deletionDate: Long,
      explicitlyDeleted: Boolean,
      `ForumId`: Long,
      `PersonId`: Long
  ) extends RawEntity

  case class ForumHasTag(
      creationDate: Long,
      deletionDate: Long,
      `ForumId`: Long,
      `TagId`: Int
  ) extends RawEntity

  case class Comment(
      creationDate: Long,
      deletionDate: Long,
      explicitlyDeleted: Boolean,
      id: Long,
      `locationIP`: String,
      `browserUsed`: String,
      `content`: String,
      `length`: Int,
      `CreatorPersonId`: Long,
      `LocationCountryId`: Int,
      `ParentPostId`: Option[Long],
      `ParentCommentId`: Option[Long]
  ) extends RawEntity

  case class CommentHasTag(
      creationDate: Long,
      deletionDate: Long,
      `CommentId`: Long,
      `TagId`: Int
  ) extends RawEntity

  case class Post(
      creationDate: Long,
      deletionDate: Long,
      explicitlyDeleted: Boolean,
      id: Long,
      imageFile: String,
      locationIP: String,
      browserUsed: String,
      language: String,
      content: String,
      length: Int,
      `CreatorPersonId`: Long,
      `ContainerForumId`: Long,
      `LocationCountryId`: Long
  ) extends RawEntity

  case class PostHasTag(
      creationDate: Long,
      deletionDate: Long,
      `PostId`: Long,
      `TagId`: Int
  ) extends RawEntity

  case class PersonLikesPost(
      creationDate: Long,
      deletionDate: Long,
      explicitlyDeleted: Boolean,
      `PersonId`: Long,
      `PostId`: Long
  ) extends RawEntity

  case class PersonLikesComment(
      creationDate: Long,
      deletionDate: Long,
      explicitlyDeleted: Boolean,
      `PersonId`: Long,
      `CommentId`: Long
  ) extends RawEntity

  case class Organisation(
      id: Int,
      `type`: String,
      name: String,
      url: String,
      `LocationPlaceId`: Int
  ) extends RawEntity

  case class Place(
      id: Int,
      name: String,
      url: String,
      `type`: String,
      `PartOfPlaceId`: Option[Int]
  ) extends RawEntity

  case class Tag(
      id: Int,
      name: String,
      url: String,
      `TypeTagClassId`: Int
  ) extends RawEntity

  case class TagClass(
      id: Int,
      name: String,
      url: String,
      `SubclassOfTagClassId`: Option[Int]
  ) extends RawEntity

  val OrganisationType            = Node("Organisation", isStatic = true)
  val PlaceType                   = Node("Place", isStatic = true)
  val TagType                     = Node("Tag", isStatic = true)
  val TagClassType                = Node("TagClass", isStatic = true)
  val CommentType                 = Node("Comment")
  val CommentHasTagType           = Edge("HasTag", "Comment", "Tag", NN)
  val ForumType                   = Node("Forum")
  val ForumHasMemberType          = Edge("HasMember", "Forum", "Person", NN)
  val ForumHasTagType             = Edge("HasTag", "Forum", "Tag", NN)
  val PersonType                  = Node("Person")
  val PersonHasInterestTagType    = Edge("HasInterest", "Person", "Tag", NN)
  val PersonKnowsPersonType       = Edge("Knows", "Person", "Person", NN)
  val PersonLikesCommentType      = Edge("Likes", "Person", "Comment", NN)
  val PersonLikesPostType         = Edge("Likes", "Person", "Post", NN)
  val PersonStudyAtUniversityType = Edge("StudyAt", "Person", "University", OneN)
  val PersonWorkAtCompanyType     = Edge("WorkAt", "Person", "Company", NN)
  val PostType                    = Node("Post")
  val PostHasTagType              = Edge("HasTag", "Post", "Tag", NN)

  trait EntityTraitsInstances {
    import EntityTraits._
    import ldbc.snb.datagen.sql._

    // static
    implicit val entityTraitsFor_TAG: EntityTraits[Tag]                   = pure(TagType, 1.0)
    implicit val entityTraitsFor_TAGCLASS: EntityTraits[TagClass]         = pure(TagClassType, 1.0)
    implicit val entityTraitsFor_PLACE: EntityTraits[Place]               = pure(PlaceType, 1.0)
    implicit val entityTraitsFor_ORGANISATION: EntityTraits[Organisation] = pure(OrganisationType, 1.0)

    // dynamic activity
    implicit val entityTraitsFor_FORUM: EntityTraits[Forum]                             = pure(ForumType, 5.13)
    implicit val entityTraitsFor_FORUM_HASMEMBER_PERSON: EntityTraits[ForumHasMember]   = pure(ForumHasMemberType, 384.06)
    implicit val entityTraitsFor_FORUM_HASTAG_TAG: EntityTraits[ForumHasTag]            = pure(ForumHasTagType, 11.10)
    implicit val entityTraitsFor_PERSON_LIKES_POST: EntityTraits[PersonLikesPost]       = pure(PersonLikesPostType, 141.12)
    implicit val entityTraitsFor_PERSON_LIKES_COMMENT: EntityTraits[PersonLikesComment] = pure(PersonLikesCommentType, 325.31)
    implicit val entityTraitsFor_POST: EntityTraits[Post]                               = pure(PostType, 138.61)
    implicit val entityTraitsFor_POST_HASTAG_TAG: EntityTraits[PostHasTag]              = pure(PostHasTagType, 77.34)
    implicit val entityTraitsFor_COMMENT: EntityTraits[Comment]                         = pure(CommentType, 503.70)
    implicit val entityTraitsFor_COMMENT_HASTAG_TAG: EntityTraits[CommentHasTag]        = pure(CommentHasTagType, 295.20)

    // dynamic person
    implicit val entityTraitsFor_PERSON: EntityTraits[Person]                                     = pure(PersonType, 1.0)
    implicit val entityTraitsFor_PERSON_HASINTEREST_TAG: EntityTraits[PersonHasInterestTag]       = pure(PersonHasInterestTagType, 7.89)
    implicit val entityTraitsFor_PERSON_WORKAT_COMPANY: EntityTraits[PersonWorkAtCompany]         = pure(PersonWorkAtCompanyType, 0.77)
    implicit val entityTraitsFor_PERSON_STUDYAT_UNIVERSITY: EntityTraits[PersonStudyAtUniversity] = pure(PersonStudyAtUniversityType, 0.28)
    implicit val entityTraitsFor_PERSON_KNOWS_PERSON: EntityTraits[PersonKnowsPerson]             = pure(PersonKnowsPersonType, 26.11)
  }

  object instances extends EntityTraitsInstances
}
