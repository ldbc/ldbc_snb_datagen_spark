package ldbc.snb.datagen.spark.transform

import ldbc.snb.datagen.model.legacy
import ldbc.snb.datagen.entities
import ldbc.snb.datagen.generator.generators.{GenActivity, GenWall}

import collection.JavaConverters._
import java.{lang, util}

trait EntityConversion[A] {
  type Repr

  def repr(self: A): Repr
}

object EntityConversion {
  type Aux[A, R] = EntityConversion[A] {type Repr = R}

  def apply[A, R](implicit instance: Aux[A, R]): Aux[A, R] = instance

  def pure[A, R](f: A => R) = new EntityConversion[A] {
    override type Repr = R

    override def repr(self: A): R = f(self)
  }

  // begin Ops boilerplate
  trait Ops[A] {
    type Repr

    def typeClassInstance: Aux[A, Repr]

    def self: A

    def repr = typeClassInstance.repr(self)
  }

  object AllOps {
    type Aux[A, R] = Ops[A] {type Repr = R}
  }

  trait ToConversionOps {
    import scala.language.implicitConversions

    implicit def toConversionOps[A, R](target: A)(implicit tc: Aux[A, R]): AllOps.Aux[A, R] = new Ops[A] {
      type Repr = R
      val self = target
      val typeClassInstance = tc
    }
  }

  object ops extends ToConversionOps
  // end Ops boilerplate
}

import EntityConversion.ops._
import EntityConversion._

trait JavaPrimitiveInstances {
  implicit val conversionForJLong = pure((_: lang.Long).longValue)

  implicit val conversionForJInteger = pure((_: lang.Integer).intValue)

  implicit val conversionForJString = pure(identity[String])
}

trait JavaCollectionInstances {
  implicit def conversionForJList[V, R](implicit ev: Aux[V, R]) = pure((_: util.List[V]).asScala.map(_.repr).toList)

  implicit def conversionForMap[K, V, KR, VR](implicit ev0: Aux[K, KR], ev1: Aux[V, VR]) =
    pure((_: util.Map[K, V]).asScala.map { case (k, v) => k.repr -> v.repr }.toMap)

  implicit def conversionForJTreeSet[V, R](implicit ev: Aux[V, R]) =
    pure((_: util.TreeSet[V]).asScala.map(_.repr).toSet)
}

trait JavaTuplesInstances {
  implicit def conversionForPair[A0, R0, A1, R1](implicit ev0: Aux[A0, R0], ev1: Aux[A1, R1]) = pure(
    (v: org.javatuples.Pair[A0, A1]) => (v.getValue0.repr, v.getValue1.repr)
  )

  implicit def conversionForTriplet[A0, R0, A1, R1, A2, R2](implicit ev0: Aux[A0, R0], ev1: Aux[A1, R1], ev2: Aux[A2, R2]) = pure(
    (v: org.javatuples.Triplet[A0, A1, A2]) => (v.getValue0.repr, v.getValue1.repr, v.getValue2.repr)
  )
}

trait LegacyEntityInstances extends JavaPrimitiveInstances with JavaCollectionInstances with JavaTuplesInstances {
  implicit val conversionForIp = pure((self: entities.dynamic.person.IP) => legacy.IP(
    ip = self.getIp,
    mask = self.getMask
  ))

  implicit val conversionForPersonSummary = pure((self: entities.dynamic.person.PersonSummary) => legacy.PersonSummary(
    accountId = self.getAccountId,
    creationDate = self.getCreationDate,
    deletionDate = self.getDeletionDate,
    browserId = self.getBrowserId,
    country = self.getCountryId,
    ipAddress = self.getIpAddress.repr,
    isLargePoster = self.getIsLargePoster,
    isMessageDeleter = self.getIsMessageDeleter
  ))

  implicit val conversionForKnows = pure((self: entities.dynamic.relations.Knows) => legacy.Knows(
    isExplicitlyDeleted = self.isExplicitlyDeleted,
    to = self.to().repr,
    creationDate = self.getCreationDate,
    deletionDate = self.getDeletionDate,
    weight = self.getWeight
  ))

  implicit val conversionForPerson = pure((self: entities.dynamic.person.Person) => legacy.Person(
    isExplicitlyDeleted = self.isExplicitlyDeleted,
    isMessageDeleter = self.isMessageDeleter,
    accountId = self.getAccountId,
    creationDate = self.getCreationDate,
    deletionDate = self.getDeletionDate,
    maxNumKnows = self.getMaxNumKnows,
    knows = self.getKnows.repr,
    browserId = self.getBrowserId,
    ipAddress = self.getIpAddress.repr,
    countryId = self.getCountryId,
    cityId = self.getCityId,
    interests = self.getInterests.repr,
    mainInterest = self.getMainInterest,
    universityLocationId = self.getUniversityLocationId,
    gender = self.getGender,
    birthday = self.getBirthday,
    isLargePoster = self.getIsLargePoster,
    randomId = self.getRandomId,
    emails = self.getEmails.repr,
    languages = self.getLanguages.repr,
    firstName = self.getFirstName,
    lastName = self.getLastName,
    companies = self.getCompanies.repr,
    classYear = self.getClassYear
  ))
  
  implicit val conversionForPost = pure((self: entities.dynamic.messages.Post) => legacy.Post(
    isExplicitlyDeleted = self.isExplicitlyDeleted,
    messageId = self.getMessageId,
    creationDate = self.getCreationDate,
    deletionDate = self.getDeletionDate,
    author = self.getAuthor.repr,
    forumId = self.getForumId,
    content = self.getContent,
    tags = self.getTags.repr,
    ipAddress = self.getIpAddress.repr,
    browserId = self.getBrowserId,
    countryId = self.getCountryId,
    language = self.getLanguage
  ))

  implicit val conversionForComment = pure((self: entities.dynamic.messages.Comment) => legacy.Comment(
    isExplicitlyDeleted = self.isExplicitlyDeleted,
    messageId = self.getMessageId,
    creationDate = self.getCreationDate,
    deletionDate = self.getDeletionDate,
    author = self.getAuthor.repr,
    forumId = self.getForumId,
    content = self.getContent,
    tags = self.getTags.repr,
    ipAddress = self.getIpAddress.repr,
    browserId = self.getBrowserId,
    countryId = self.getCountryId,
    rootPostId = self.rootPostId,
    parentMessageId = self.parentMessageId
  ))

  implicit val conversionForPhoto = pure((self: entities.dynamic.messages.Photo) => legacy.Photo(
    isExplicitlyDeleted = self.isExplicitlyDeleted,
    messageId = self.getMessageId,
    creationDate = self.getCreationDate,
    deletionDate = self.getDeletionDate,
    author = self.getAuthor.repr,
    forumId = self.getForumId,
    content = self.getContent,
    tags = self.getTags.repr,
    ipAddress = self.getIpAddress.repr,
    browserId = self.getBrowserId,
    countryId = self.getCountryId
  ))

  implicit val conversionForForumType = pure((self: entities.dynamic.Forum.ForumType) => legacy.Forum.Type(self.ordinal))

  implicit val conversionForForumMembership = pure((self: entities.dynamic.relations.ForumMembership) => legacy.ForumMembership(
    isExplicitlyDeleted = self.isExplicitlyDeleted,
    forumId = self.getForumId,
    creationDate = self.getCreationDate,
    deletionDate = self.getDeletionDate,
    person = self.getPerson.repr,
    forumType = self.getForumType.repr
  ))
  
  implicit val conversionForForum = pure((self: entities.dynamic.Forum) => legacy.Forum(
    isExplicitlyDeleted = self.isExplicitlyDeleted,
    id = self.getId,
    moderator = self.getModerator.repr,
    moderatorDeletionDate = self.getModeratorDeletionDate,
    creationDate = self.getCreationDate,
    deletionDate = self.getDeletionDate,
    title = self.getTitle,
    tags = self.getTags.repr,
    placeId = self.getPlace,
    language = self.getLanguage,
    memberships = self.getMemberships.repr,
    forumType = self.getForumType.repr
  ))

  implicit val conversionForLikeType = pure((self: entities.dynamic.relations.Like.LikeType) => legacy.Like.Type(self.ordinal))
  
  implicit val conversionForLike = pure((self: entities.dynamic.relations.Like) => legacy.Like(
    isExplicitlyDeleted = self.isExplicitlyDeleted,
    person = self.getPerson,
    personCreationDate = self.getPersonCreationDate,
    messageId = self.getMessageId,
    creationDate = self.getCreationDate,
    deletionDate = self.getDeletionDate,
    `type` = self.getType.repr
  ))


  // eliminate the GenWall wrapper completely
  implicit def conversionForWall[A, R](implicit ev: Aux[A, R]) = pure((wall: GenWall[A]) => wall.inner.repr)

  implicit val conversionForGenActivities = pure((self: GenActivity) => legacy.Activity(
    wall = self.genWall.repr,
    groups = self.genGroups.repr,
    albums = self.genAlbums.repr
  ))
}

object EntityConversionInstances extends LegacyEntityInstances


