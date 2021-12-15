package ldbc.snb.datagen.generator.serializers

import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.dictionary.Dictionaries
import ldbc.snb.datagen.io.raw.RecordOutputStream
import ldbc.snb.datagen.model.raw

import java.util
import scala.collection.JavaConverters._

class PersonOutputStream(
    personStream: RecordOutputStream[raw.Person],
    personKnowsPersonStream: RecordOutputStream[raw.PersonKnowsPerson],
    personHasInterestTagStream: RecordOutputStream[raw.PersonHasInterestTag],
    personStudyAtUniversityStream: RecordOutputStream[raw.PersonStudyAtUniversity],
    personWorkAtCompanyStream: RecordOutputStream[raw.PersonWorkAtCompany]
) extends RecordOutputStream[Person] {

  private def getGender(gender: Int): String = if (gender == 0) "female" else "male"

  private def getLanguages(languages: util.List[Integer]): Seq[String] = {
    languages.asScala.map(x => Dictionaries.languages.getLanguageName(x))
  }

  private def getEmail(emails: util.List[String]): Seq[String] = emails.asScala

  private def getPerson(person: Person) = raw.Person(
    person.getCreationDate,
    person.getDeletionDate,
    person.isExplicitlyDeleted,
    person.getAccountId,
    person.getFirstName,
    person.getLastName,
    getGender(person.getGender),
    person.getBirthday,
    person.getIpAddress.toString,
    Dictionaries.browsers.getName(person.getBrowserId),
    person.getCityId,
    getLanguages(person.getLanguages).mkString(";"),
    getEmail(person.getEmails).mkString(";")
  )

  def write(person: Person): Unit = {

    val p = getPerson(person)
    personStream.write(p)

    for (interestIdx <- person.getInterests.iterator().asScala) {
      val personHasInterestTag = raw.PersonHasInterestTag(
        person.getCreationDate,
        person.getDeletionDate,
        person.getAccountId,
        interestIdx
      )
      personHasInterestTagStream.write(personHasInterestTag)
    }

    val universityId = Dictionaries.universities.getUniversityFromLocation(person.getUniversityLocationId)
    if ((universityId != -1) && (person.getClassYear != -1)) {
      val personStudyAtUniversity = raw.PersonStudyAtUniversity(
        person.getCreationDate,
        person.getDeletionDate,
        person.getAccountId,
        universityId,
        person.getClassYear.toInt
      )
      personStudyAtUniversityStream.write(personStudyAtUniversity)
    }

    for (companyId <- person.getCompanies.keySet.iterator().asScala) {
      val personWorkAtCompany = raw.PersonWorkAtCompany(
        person.getCreationDate,
        person.getDeletionDate,
        person.getAccountId,
        companyId,
        person.getCompanies.get(companyId).toInt
      )
      personWorkAtCompanyStream.write(personWorkAtCompany)
    }

    val knows = person.getKnows

    for (know <- knows.iterator().asScala) {
      if (person.getAccountId < know.to.getAccountId) {
        val personKnowsPerson = raw.PersonKnowsPerson(
          know.getCreationDate,
          know.getDeletionDate,
          know.isExplicitlyDeleted,
          person.getAccountId,
          know.to().getAccountId
        )
        personKnowsPersonStream.write(personKnowsPerson)
      }
    }
  }

  override def close(): Unit = {
    personStream.close()
    personKnowsPersonStream.close()
    personHasInterestTagStream.close()
    personStudyAtUniversityStream.close()
    personWorkAtCompanyStream.close()
  }
}
