package ldbc.snb.datagen.io.raw.csv

import ldbc.snb.datagen.io.raw.csv.CsvRowEncoderInstances.FieldNames
import ldbc.snb.datagen.model.raw._
import ldbc.snb.datagen.util.formatter.DateFormatter
import shapeless._
import shapeless.labelled.FieldType

// traits can't have context bounds in Scala 2 :(
abstract class CsvRowEncoderWithAutoHeader[T: FieldNames] extends CsvRowEncoder[T] {
  override def header: Seq[String] = FieldNames[T].value
}

trait CsvRowEncoderInstances {
  private[this] val dateFormatter = new DateFormatter

  implicit object PersonCsvRowEncoder extends CsvRowEncoderWithAutoHeader[Person] {
    override def row(person: Person): Seq[String] = Array(
      person.creationDate.toString,
      person.deletionDate.toString,
      String.valueOf(person.explicitlyDeleted),
      person.id.toString,
      person.firstName,
      person.lastName,
      person.gender,
      dateFormatter.formatDate(person.birthday),
      person.`locationIP`,
      person.`browserUsed`,
      Integer.toString(person.`place`),
      person.`language`,
      person.`email`
    )
  }

  implicit object PersonKnowsPersonCsvRowEncoder extends CsvRowEncoderWithAutoHeader[PersonKnowsPerson] {
    override def row(t: PersonKnowsPerson): Seq[String] = Array(
      t.creationDate.toString,
      t.deletionDate.toString,
      String.valueOf(t.explicitlyDeleted),
      t.`Person1Id`.toString,
      t.`Person2Id`.toString
    )
  }

  implicit object PersonHasInterestCsvRowEncoder extends CsvRowEncoderWithAutoHeader[PersonHasInterestTag] {
    override def row(t: PersonHasInterestTag): Seq[String] = Array(
      t.creationDate.toString,
      t.deletionDate.toString,
      t.personId.toString,
      t.interestIdx.toString
    )
  }

  implicit object PersonStudyAtUniversityCsvRowEncoder extends CsvRowEncoderWithAutoHeader[PersonStudyAtUniversity] {
    override def row(t: PersonStudyAtUniversity): Seq[String] = Array(
      t.creationDate.toString,
      t.deletionDate.toString,
      t.PersonId.toString,
      t.UniversityId.toString,
      t.classYear.toString
    )
  }

  implicit object PersonWorkAtCompanyEncoder extends CsvRowEncoderWithAutoHeader[PersonWorkAtCompany] {
    override def row(t: PersonWorkAtCompany): Seq[String] = Array(
      t.creationDate.toString,
      t.deletionDate.toString,
      t.PersonId.toString,
      t.CompanyId.toString,
      t.workFrom.toString
    )
  }

  implicit object ForumEncoder extends CsvRowEncoderWithAutoHeader[Forum] {
    override def row(t: Forum): Seq[String] =
      Array(
        t.creationDate.toString,
        t.deletionDate.toString,
        t.explicitlyDeleted.toString,
        t.id.toString,
        t.title,
        t.`ModeratorPersonId`.toString
      )
  }

  implicit object ForumHasTagEncoder extends CsvRowEncoderWithAutoHeader[ForumHasTag] {
    override def row(t: ForumHasTag): Seq[String] = Array(
      t.creationDate.toString,
      t.deletionDate.toString,
      t.`ForumId`.toString,
      t.`TagId`.toString
    )
  }

  implicit object PostEncoder extends CsvRowEncoderWithAutoHeader[Post] {
    override def row(t: Post): Seq[String] = Array(
      t.creationDate.toString,
      t.deletionDate.toString,
      t.explicitlyDeleted.toString,
      t.id.toString,
      t.imageFile,
      t.locationIP,
      t.browserUsed,
      t.language,
      t.content,
      t.length.toString,
      t.`CreatorPersonId`.toString,
      t.`ContainerForumId`.toString,
      t.`LocationCountryId`.toString
    )
  }

  implicit object PostHasTagEncoder extends CsvRowEncoderWithAutoHeader[PostHasTag] {
    override def row(t: PostHasTag): Seq[String] = Array(
      t.creationDate.toString,
      t.deletionDate.toString,
      t.`PostId`.toString,
      t.`TagId`.toString
    )
  }

  implicit object CommentEncoder extends CsvRowEncoderWithAutoHeader[Comment] {
    override def row(t: Comment): Seq[String] = Array(
      t.creationDate.toString,
      t.deletionDate.toString,
      t.explicitlyDeleted.toString,
      t.id.toString,
      t.locationIP,
      t.browserUsed,
      t.`content`,
      t.length.toString,
      t.`CreatorPersonId`.toString,
      t.`LocationCountryId`.toString,
      t.`ParentPostId`.fold("")(_.toString),
      t.`ParentCommentId`.fold("")(_.toString)
    )
  }

  implicit object CommentHasTagEncoder extends CsvRowEncoderWithAutoHeader[CommentHasTag] {
    override def row(t: CommentHasTag): Seq[String] = Array(
      t.creationDate.toString,
      t.deletionDate.toString,
      t.`CommentId`.toString,
      t.`TagId`.toString
    )
  }

  implicit object ForumHasMemberEncoder extends CsvRowEncoderWithAutoHeader[ForumHasMember] {
    override def row(t: ForumHasMember): Seq[String] = Array(
      t.creationDate.toString,
      t.deletionDate.toString,
      t.`ForumId`.toString,
      t.`PersonId`.toString
    )
  }

  implicit object PersonLikesPostEncoder extends CsvRowEncoderWithAutoHeader[PersonLikesPost] {
    override def row(t: PersonLikesPost): Seq[String] = Array(
      t.creationDate.toString,
      t.deletionDate.toString,
      t.`PersonId`.toString,
      t.`PostId`.toString
    )
  }

  implicit object PersonLikesCommentEncoder extends CsvRowEncoderWithAutoHeader[PersonLikesComment] {
    override def row(t: PersonLikesComment): Seq[String] = Array(
      t.creationDate.toString,
      t.deletionDate.toString,
      t.`PersonId`.toString,
      t.`CommentId`.toString
    )
  }

  implicit object PlaceEncoder extends CsvRowEncoderWithAutoHeader[Place] {
    override def row(t: Place): Seq[String] = Array(
      t.id.toString,
      t.name,
      t.url,
      t.`type`,
      t.`PartOfPlaceId`.fold("")(_.toString)
    )
  }

  implicit object TagEncoder extends CsvRowEncoderWithAutoHeader[Tag] {
    override def row(t: Tag): Seq[String] = Array(
      t.id.toString,
      t.name,
      t.url,
      t.`TypeTagClassId`.toString
    )
  }

  implicit object TagClassEncoder extends CsvRowEncoderWithAutoHeader[TagClass] {
    override def row(t: TagClass): Seq[String] = Array(
      t.id.toString,
      t.name,
      t.url,
      t.`SubclassOfTagClassId`.fold("")(_.toString)
    )
  }

  implicit object OrganisationEncoder extends CsvRowEncoderWithAutoHeader[Organisation] {
    override def row(t: Organisation): Seq[String] = Array(
      t.id.toString,
      t.name,
      t.url,
      t.`LocationPlaceId`.toString
    )
  }

}

object CsvRowEncoderInstances {
  sealed trait FieldNames[T] { def value: List[String] }

  object FieldNames {
    def apply[T: FieldNames] = implicitly[FieldNames[T]]

    implicit val hnil: FieldNames[HNil] = new FieldNames[HNil] { val value = Nil }

    implicit def hcons[K <: Symbol, V, T <: HList](implicit wit: Witness.Aux[K], rest: FieldNames[T]): FieldNames[FieldType[K, V] :: T] =
      new FieldNames[FieldType[K, V] :: T] { val value = wit.value.name +: rest.value }

    implicit def caseClass[T, G](implicit lg: LabelledGeneric.Aux[T, G], rest: FieldNames[G]): FieldNames[T] = new FieldNames[T] {
      val value = rest.value
    }
  }
}
