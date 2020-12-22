package ldbc.snb.datagen.spark.transform

import ldbc.snb.datagen.model.legacy
import ldbc.snb.datagen.entities
import ldbc.snb.datagen.entities.dynamic.person.PersonSummary

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
  implicit def conversionForJLong = pure((_: lang.Long).longValue)

  implicit def conversionForJInteger = pure((_: lang.Integer).intValue)

  implicit def conversionForJString = pure(identity[String])
}

trait JavaCollectionInstances {
  implicit def conversionForJList[V, R](implicit ev: Aux[V, R]) = pure((_: util.List[V]).asScala.map(_.repr).toList)

  implicit def conversionForMap[K, V, KR, VR](implicit ev0: Aux[K, KR], ev1: Aux[V, VR]) =
    pure((_: util.Map[K, V]).asScala.map { case (k, v) => k.repr -> v.repr }.toMap)

  implicit def conversionForJTreeSet[V, R](implicit ev: Aux[V, R]) =
    pure((_: util.TreeSet[V]).asScala.map(_.repr).toSet)
}

trait LegacyEntityInstances extends JavaPrimitiveInstances with JavaCollectionInstances {
  implicit val conversionForIp = pure((self: entities.dynamic.person.IP) => legacy.IP(
    ip = self.getIp,
    mask = self.getMask
  ))

  implicit val conversionForPersonSummary = pure((self: PersonSummary) => legacy.PersonSummary(
    accountId = self.getAccountId,
    creationDate = self.getCreationDate,
    deletionDate = self.getDeletionDate,
    browserId = self.getBrowserId,
    country = self.getCountryId,
    ipAddress = self.getIpAddress.repr,
    isLargePoster = self.getIsLargePoster,
    isMessageDeleter = self.getIsMessageDeleter
  ))

  implicit def conversionForKnows = pure((self: entities.dynamic.relations.Knows) => legacy.Knows(
    isExplicitlyDeleted = self.isExplicitlyDeleted,
    to = self.to().repr,
    creationDate = self.getCreationDate,
    deletionDate = self.getDeletionDate,
    weight = self.getWeight
  ))

  implicit def conversionForPerson = pure((self: entities.dynamic.person.Person) => legacy.Person(
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
}

object EntityConversionInstances
  extends JavaPrimitiveInstances
    with JavaCollectionInstances
    with LegacyEntityInstances


