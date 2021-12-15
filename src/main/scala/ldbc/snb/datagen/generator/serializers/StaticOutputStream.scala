package ldbc.snb.datagen.generator.serializers

import ldbc.snb.datagen.entities.statictype.Organisation
import ldbc.snb.datagen.entities.statictype.place.Place
import ldbc.snb.datagen.generator.dictionary.Dictionaries
import ldbc.snb.datagen.generator.vocabulary.{DBP, DBPOWL}
import ldbc.snb.datagen.io.raw.RecordOutputStream
import ldbc.snb.datagen.model.raw
import ldbc.snb.datagen.util.StringUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

// Marker type for the static graph
object StaticGraph

class StaticOutputStream(
    placeStream: RecordOutputStream[raw.Place],
    tagStream: RecordOutputStream[raw.Tag],
    tagClassStream: RecordOutputStream[raw.TagClass],
    organisationStream: RecordOutputStream[raw.Organisation]
) extends RecordOutputStream[StaticGraph.type] {

  val exportedClasses: mutable.Set[Int] = mutable.Set.empty

  private def writePlace(place: Place) = {
    val partOfPlaceId = place.getType match {
      case Place.CITY | Place.COUNTRY => Some(Dictionaries.places.belongsTo(place.getId))
      case _                          => None
    }

    val rawPlace = raw.Place(
      place.getId,
      place.getName,
      DBP.getUrl(place.getName),
      place.getType,
      partOfPlaceId
    )

    placeStream.write(rawPlace)
  }

  def writeTagHierarchy(tag: raw.Tag): Unit = {
    val classId = tag.`TypeTagClassId`
    while (classId != -1 && !exportedClasses.contains(classId)) {
      exportedClasses.add(classId)
      val classParent = Dictionaries.tags.getClassParent(classId)
      val className   = StringUtils.clampString(Dictionaries.tags.getClassName(classId), 256)
      val rawTagClass = raw.TagClass(
        classId,
        className,
        if (className == "Thing") "http://www.w3.org/2002/07/owl#Thing" else DBPOWL.getUrl(className),
        if (classParent != -1) Some(classParent) else None
      )
      tagClassStream.write(rawTagClass)
    }
  }

  override def write(s: StaticGraph.type): Unit = {
    val locations = Dictionaries.places.getPlaces.iterator().asScala

    for { location <- locations } {
      val place = Dictionaries.places.getLocation(location)
      place.setName(StringUtils.clampString(place.getName, 256))
      writePlace(place)
    }

    val tags = Dictionaries.tags.getTags.iterator().asScala

    for { tag <- tags } {
      val tagName = StringUtils
        .clampString(Dictionaries.tags.getName(tag), 256)
        .replace("\"", "\\\"")
      val rawTag = raw.Tag(
        tag,
        tagName,
        DBP.getUrl(tagName),
        Dictionaries.tags.getTagClass(tag)
      )
      tagStream.write(rawTag)
      writeTagHierarchy(rawTag)
    }

    val companies = Dictionaries.companies.getCompanies.iterator().asScala

    for { company <- companies } {
      val companyName = StringUtils.clampString(Dictionaries.companies.getCompanyName(company), 256)
      val rawOrganisation = raw.Organisation(
        company.toInt,
        Organisation.OrganisationType.Company.toString,
        companyName,
        DBP.getUrl(companyName),
        Dictionaries.companies.getCountry(company)
      )
      organisationStream.write(rawOrganisation)
    }

    val universities = Dictionaries.universities.getUniversities.iterator().asScala

    for { university <- universities } {
      val universityName = StringUtils.clampString(Dictionaries.universities.getUniversityName(university), 256)
      val rawOrganisation = raw.Organisation(
        university.toInt,
        Organisation.OrganisationType.University.toString,
        universityName,
        DBP.getUrl(universityName),
        Dictionaries.universities.getUniversityCity(university)
      )
      organisationStream.write(rawOrganisation)
    }
  }

  override def close(): Unit = {
    placeStream.close()
    tagStream.close()
    tagClassStream.close()
    organisationStream.close()
  }
}
