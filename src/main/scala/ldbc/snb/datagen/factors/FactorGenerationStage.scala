package ldbc.snb.datagen.factors

import ldbc.snb.datagen.factors.io.FactorTableSink
import ldbc.snb.datagen.io.graphs.GraphSource
import ldbc.snb.datagen.model
import ldbc.snb.datagen.model.EntityType
import ldbc.snb.datagen.model.Mode.Raw
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.transformation.transform.ConvertDates
import ldbc.snb.datagen.util.{DatagenStage, Logging}
import org.apache.spark.graphx
import org.apache.spark.sql.functions.{broadcast, col, count, date_trunc, expr, floor, lit, sum}
import org.apache.spark.sql.{Column, DataFrame, Row, functions}
import shapeless._

import scala.util.matching.Regex

trait FactorTrait extends (Seq[DataFrame] => DataFrame) {
  def requiredEntities: Seq[EntityType]
}

case class Factor(override val requiredEntities: EntityType*)(f: Seq[DataFrame] => DataFrame) extends FactorTrait {
  override def apply(v1: Seq[DataFrame]): DataFrame = f(v1).coalesce(1)
}

case class LargeFactor(override val requiredEntities: EntityType*)(f: Seq[DataFrame] => DataFrame) extends FactorTrait {
  override def apply(v1: Seq[DataFrame]): DataFrame = f(v1)
}

object FactorGenerationStage extends DatagenStage with Logging {

  case class Args(
      outputDir: String = "out",
      irFormat: String = "parquet",
      format: String = "parquet",
      only: Option[Regex] = None,
      force: Boolean = false
  )

  override type ArgsType = Args

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Args](getClass.getName.dropRight(1)) {
      head(appName)

      val args = lens[Args]

      opt[String]('o', "output-dir")
        .action((x, c) => args.outputDir.set(c)(x))
        .text(
          "path on the cluster filesystem, where Datagen outputs. Can be a URI (e.g S3, ADLS, HDFS) or a " +
            "path in which case the default cluster file system is used."
        )

      opt[String]("ir-format")
        .action((x, c) => args.irFormat.set(c)(x))
        .text("Format of the raw input")

      opt[String]("format")
        .action((x, c) => args.format.set(c)(x))
        .text("Output format")

      opt[String]("only")
        .action((x, c) => args.only.set(c)(Some(x.r.anchored)))
        .text("Only generate factor tables whose name matches the supplied regex")

      opt[Unit]("force")
        .action((_, c) => args.force.set(c)(true))
        .text("Overwrites existing output")

      help('h', "help").text("prints this usage text")
    }

    val parsedArgs = parser.parse(args, Args()).getOrElse(throw new RuntimeException("Invalid arguments"))

    run(parsedArgs)
  }

  def run(args: Args): Unit = {
    import ldbc.snb.datagen.factors.io.instances._
    import ldbc.snb.datagen.io.Reader.ops._
    import ldbc.snb.datagen.io.Writer.ops._
    import ldbc.snb.datagen.io.instances._
    import ldbc.snb.datagen.transformation.transform.ConvertDates.instances._

    GraphSource(model.graphs.Raw.graphDef, args.outputDir, args.irFormat).read
      .pipe(ConvertDates[Raw.type].transform)
      .pipe(g =>
        rawFactors
          .collect {
            case (name, calc) if args.only.fold(true)(_.findFirstIn(name).isDefined) =>
              val resolvedEntities = calc.requiredEntities.foldLeft(Seq.empty[DataFrame])((args, et) => args :+ g.entities(et))
              FactorTable(name, calc(resolvedEntities), g)
          }
      )
      .foreach(_.write(FactorTableSink(args.outputDir, format = args.format, overwrite = args.force)))
  }

  private def frequency(df: DataFrame, value: Column, by: Seq[Column], agg: Column => Column = count) =
    df
      .groupBy(by: _*)
      .agg(agg(value).as("frequency"))
      .select(by :+ $"frequency": _*)
      .orderBy($"frequency".desc +: by.map(_.asc): _*)

  private def undirectedKnows(personKnowsPerson: DataFrame) =
    personKnowsPerson
      .select(expr("stack(2, Person1Id, Person2Id, Person2Id, Person1Id)").as(Seq("Person1Id", "Person2Id")))
      .alias("Knows")
      .cache()

  private def undirectedKnowsTemporal(personKnowsPerson: DataFrame) =
    personKnowsPerson
      .select(
        expr("stack(2, Person1Id, Person2Id, creationDate, deletionDate, Person2Id, Person1Id, creationDate, deletionDate)")
        .as(Seq("Person1Id", "Person2Id", "creationDate", "deletionDate"))
      )
      .alias("Knows")
      .cache()

  private def nHops(relationships: DataFrame, n: Int, joinKeys: (String, String), sample: Option[DataFrame => DataFrame] = None): DataFrame = {
    val (leftKey, rightKey) = joinKeys
    relationships
      .pipeFoldLeft(sample) { (df, sampler) => sampler(df) }
      .withColumn("nhops", lit(1))
      .pipeFoldLeft(1 until n) { (df, count) =>
        df
          .where($"nhops" === count)
          .alias("left")
          .join(relationships.alias("right"), $"left.${leftKey}" === $"right.${rightKey}")
          .select($"left.${rightKey}".alias(rightKey), $"right.${leftKey}".alias(leftKey), lit(count + 1).alias("nhops"))
          .unionAll(df)
          .groupBy(Seq(rightKey, leftKey).map(col): _*)
          .agg(functions.min("nhops").alias("nhops"))
      }
      .where($"nhops" === n)
      .select(Seq(rightKey, leftKey).map(col): _*)
  }

  private def messageTags(commentHasTag: DataFrame, postHasTag: DataFrame, tag: DataFrame) = {
    val messageHasTag = commentHasTag.select($"CommentId".as("id"), $"TagId") |+| postHasTag.select($"PostId".as("id"), $"TagId")

    frequency(
      messageHasTag.as("MessageHasTag").join(tag.as("Tag"), $"Tag.id" === $"MessageHasTag.TagId"),
      value = $"MessageHasTag.TagId",
      by = Seq($"Tag.id", $"Tag.name")
    ).select($"Tag.id".as("tagId"), $"Tag.name".as("tagName"), $"frequency")
  }

  private def sameUniversityKnows(personKnowsPerson: DataFrame, studyAt: DataFrame) = {
    undirectedKnowsTemporal(personKnowsPerson)
      .join(studyAt.as("studyAt1"), $"studyAt1.personId" === $"knows.person1Id")
      .join(studyAt.as("studyAt2"), $"studyAt2.personId" === $"knows.person2Id")
      .where($"studyAt1.universityId" === $"studyAt2.universityId")
      .select(
        $"knows.person1Id".as("person1Id"),
        $"knows.person2Id".as("person2Id"),
        functions.greatest($"knows.creationDate", $"studyAt1.creationDate", $"studyAt2.creationDate").alias("creationDate"),
        functions.least($"knows.deletionDate", $"studyAt1.deletionDate", $"studyAt2.deletionDate").alias("deletionDate")
      )
      .where($"creationDate" < $"deletionDate")
  }

  import model.raw._

  private val rawFactors = Map(
    "messageIds" -> Factor(CommentType, PostType) { case Seq(comments, posts) =>
      val messages =
        (comments.select($"creationDate", $"deletionDate", $"id".as("MessageId"))
        |+| posts.select($"creationDate", $"deletionDate", $"id".as("MessageId"))
        )
        .select(
          date_trunc("day", $"creationDate").as("creationDay"),
          date_trunc("day", $"deletionDate").as("deletionDay"),
          $"MessageId")
        .orderBy($"MessageId")

      val sampleSize = 20000.0
      val count = messages.count()
      val sampleFraction = Math.min(sampleSize / count, 1.0)
      messages.sample(sampleFraction, 42)
    },
    "countryNumPersons" -> Factor(PlaceType, PersonType) { case Seq(places, persons) =>
      val cities    = places.where($"type" === "City").cache()
      val countries = places.where($"type" === "Country").cache()

      frequency(
        persons
          .as("Person")
          .join(broadcast(cities.as("City")), $"City.id" === $"Person.LocationCityId")
          .join(broadcast(countries.as("Country")), $"Country.id" === $"City.PartOfPlaceId"),
        value = $"Person.id",
        by = Seq($"Country.id", $"Country.name")
      )
    },
    "cityNumPersons" -> Factor(PlaceType, PersonType) { case Seq(places, persons) =>
      val cities = places.where($"type" === "City").cache()

      frequency(
        persons
          .as("Person")
          .join(broadcast(cities.as("City")), $"City.id" === $"Person.LocationCityId"),
        value = $"Person.id",
        by = Seq($"City.id", $"City.name")
      )
    },
    "countryNumMessages" -> Factor(CommentType, PostType, PlaceType) { case Seq(comments, posts, places) =>
      val countries = places.where($"type" === "Country").cache()

      frequency(
        (comments.select($"id".as("MessageId"), $"LocationCountryId") |+| posts.select($"id".as("MessageId"), $"LocationCountryId"))
          .join(broadcast(countries.as("Country")), $"Country.id" === $"LocationCountryId"),
        value = $"MessageId",
        by = Seq($"LocationCountryId", $"Country.name")
      )
    },
    "cityPairsNumFriends" -> Factor(PersonKnowsPersonType, PersonType, PlaceType) { case Seq(personKnowsPerson, persons, places) =>
      val cities    = places.where($"type" === "City").cache()
      val knows     = undirectedKnows(personKnowsPerson)
      val countries = places.where($"type" === "Country").cache()

      frequency(
        knows
          .join(persons.cache().as("Person1"), $"Person1.id" === $"Knows.Person1Id")
          .join(cities.as("City1"), $"City1.id" === $"Person1.LocationCityId")
          .join(persons.as("Person2"), $"Person2.id" === $"Knows.Person2Id")
          .join(cities.as("City2"), $"City2.id" === $"Person2.LocationCityId")
          .where($"City1.id" < $"City2.id")
          .join(countries.as("Country1"), $"Country1.id" === $"City1.PartOfPlaceId")
          .join(countries.as("Country2"), $"Country2.id" === $"City2.PartOfPlaceId"),
        value = $"*",
        by = Seq($"City1.id", $"City2.id", $"City1.name", $"City2.name", $"Country1.id", $"Country2.id", $"Country1.name", $"Country2.name")
      ).select(
        $"City1.id".alias("city1Id"),
        $"City2.id".alias("city2Id"),
        $"City1.name".alias("city1Name"),
        $"City2.name".alias("city2Name"),
        $"Country1.id".alias("country1Id"),
        $"Country2.id".alias("country2Id"),
        $"Country1.name".alias("country1Name"),
        $"Country2.name".alias("country2Name"),
        $"frequency"
      )
    },
    "countryPairsNumFriends" -> Factor(PersonKnowsPersonType, PersonType, PlaceType) { case Seq(personKnowsPerson, persons, places) =>
      val cities    = places.where($"type" === "City").cache()
      val countries = places.where($"type" === "Country").cache()
      val knows     = undirectedKnows(personKnowsPerson)

      frequency(
        knows
          .join(persons.cache().as("Person1"), $"Person1.id" === $"Knows.Person1Id")
          .join(cities.as("City1"), $"City1.id" === $"Person1.LocationCityId")
          .join(persons.as("Person2"), $"Person2.id" === $"Knows.Person2Id")
          .join(cities.as("City2"), $"City2.id" === $"Person2.LocationCityId")
          .join(countries.as("Country1"), $"Country1.id" === $"City1.PartOfPlaceId")
          .join(countries.as("Country2"), $"Country2.id" === $"City2.PartOfPlaceId")
          .where($"Country1.id" < $"Country2.id"),
        value = $"*",
        by = Seq($"Country1.id", $"Country2.id", $"Country1.name", $"Country2.name")
      ).select(
        $"Country1.id".alias("country1Id"),
        $"Country2.id".alias("country2Id"),
        $"Country1.name".alias("country1Name"),
        $"Country2.name".alias("country2Name"),
        $"frequency"
      )
    },
    "creationDayNumMessages" -> Factor(CommentType, PostType) { case Seq(comments, posts) =>
      frequency(
        (comments.select($"id".as("MessageId"), $"creationDate") |+| posts.select($"id".as("MessageId"), $"creationDate"))
          .select($"MessageId", date_trunc("day", $"creationDate").as("creationDay")),
        value = $"MessageId",
        by = Seq($"creationDay")
      )
    },
    "creationDayAndTagClassNumMessages" -> Factor(CommentType, PostType, CommentHasTagType, PostHasTagType, TagType, TagClassType) {
      case Seq(comments, posts, commentHasTag, postHasTag, tag, tagClass) =>
        val messageHasTag = commentHasTag.select($"CommentId".as("id"), $"TagId") |+| postHasTag.select($"PostId".as("id"), $"TagId")
        frequency(
          (comments.select($"id".as("MessageId"), $"creationDate") |+| posts.select($"id".as("MessageId"), $"creationDate"))
            .select($"MessageId", date_trunc("day", $"creationDate").as("creationDay"))
            .join(messageHasTag.as("hasTag"), $"hasTag.id" === $"MessageId")
            .join(tag.as("Tag"), $"Tag.id" === $"hasTag.TagId")
            .join(tagClass.as("TagClass"), $"Tag.TypeTagClassId" === $"TagClass.id"),
          value = $"MessageId",
          by = Seq($"creationDay", $"TagClass.id", $"TagClass.name")
        )
    },
    "creationDayAndTagNumMessages" -> Factor(CommentType, PostType, CommentHasTagType, PostHasTagType, TagType) {
      case Seq(comments, posts, commentHasTag, postHasTag, tag) =>
        val messageHasTag = commentHasTag.select($"CommentId".as("id"), $"TagId") |+| postHasTag.select($"PostId".as("id"), $"TagId")
        frequency(
          (comments.select($"id".as("MessageId"), $"creationDate") |+| posts.select($"id".as("MessageId"), $"creationDate"))
            .select($"MessageId", date_trunc("day", $"creationDate").as("creationDay"))
            .join(messageHasTag.as("hasTag"), $"hasTag.id" === $"MessageId")
            .join(tag.as("Tag"), $"Tag.id" === $"hasTag.TagId"),
          value = $"MessageId",
          by = Seq($"creationDay", $"Tag.id", $"Tag.name")
        )
    },
    "creationDayAndLengthCategoryNumMessages" -> Factor(CommentType, PostType) { case Seq(comments, posts) =>
      frequency(
        (comments.select($"id".as("MessageId"), $"creationDate", $"length")
          |+| posts.select($"id".as("MessageId"), $"creationDate", $"length"))
          .select(
            $"MessageId",
            date_trunc("day", $"creationDate").as("creationDay"),
            floor(col("length") / 10).as("lengthCategory")
          ),
        value = $"MessageId",
        by = Seq($"creationDay", $"lengthCategory")
      )
    },
    "lengthNumMessages" -> Factor(CommentType, PostType) { case Seq(comments, posts) =>
      frequency(
        comments.select($"id", $"length") |+| posts.select($"id", $"length"),
        value = $"id",
        by = Seq($"length")
      )
    },
    "tagNumMessages" -> Factor(CommentHasTagType, PostHasTagType, TagType) { case Seq(commentHasTag, postHasTag, tag) =>
      messageTags(commentHasTag, postHasTag, tag).cache()
    },
    "tagClassNumMessages" -> Factor(CommentHasTagType, PostHasTagType, TagType, TagClassType) { case Seq(commentHasTag, postHasTag, tag, tagClass) =>
      frequency(
        messageTags(commentHasTag, postHasTag, tag)
          .as("MessageTags")
          .join(tag.as("Tag"), $"MessageTags.tagId" === $"Tag.id")
          .join(tagClass.as("TagClass"), $"Tag.TypeTagClassId" === $"TagClass.id"),
        value = $"frequency",
        by = Seq($"TagClass.id", $"TagClass.name"),
        agg = sum
      )
    },
    "personNumFriends" -> Factor(PersonKnowsPersonType, PersonType) { case Seq(personKnowsPerson, person1) =>
      val knows = person1
        .as("Person1")
        .join(undirectedKnows(personKnowsPerson).as("knows"), $"Person1.id" === $"knows.Person1Id", "leftouter")
      frequency(knows, value = $"knows.Person2Id", by = Seq($"Person1.id", $"Person1.creationDate", $"Person1.deletionDate"))
    },
    "languageNumPosts" -> Factor(PostType) { case Seq(post) =>
      frequency(post.where($"language".isNotNull), value = $"id", by = Seq($"language"))
    },
    "tagNumPersons" -> Factor(PersonHasInterestTagType, TagType) { case Seq(interest, tag) =>
      frequency(
        interest.join(tag.as("Tag"), $"Tag.id" === $"TagId"),
        value = $"PersonId",
        by = Seq($"Tag.id", $"Tag.name")
      )
    },
    "tagClassNumTags" -> Factor(TagClassType, TagType) { case Seq(tagClass, tag) =>
      frequency(
        tag.as("Tag").join(tagClass.as("TagClass"), $"Tag.TypeTagClassId" === $"TagClass.id"),
        value = $"Tag.id",
        by = Seq($"TagClass.id", $"TagClass.name")
      )
    },
    "personDisjointEmployerPairs" -> Factor(PersonType, PersonKnowsPersonType, OrganisationType, PersonWorkAtCompanyType) {
      case Seq(person, personKnowsPerson, organisation, workAt) =>
        val knows = undirectedKnows(personKnowsPerson)

        val company = organisation.where($"Type" === "Company").cache()
        val personSample = person
          .orderBy($"id")
          .limit(20)
        personSample
          .as("Person2")
          .join(knows.as("knows"), $"knows.person2Id" === $"Person2.id")
          .join(workAt.as("workAt"), $"workAt.PersonId" === $"knows.Person1id")
          .join(company.as("Company"), $"Company.id" === $"workAt.CompanyId")
          .select(
            $"Person2.id".alias("person2id"),
            $"Company.name".alias("companyName"),
            $"Company.id".alias("companyId"),
            $"Person2.creationDate".alias("person2creationDate"),
            $"Person2.deletionDate".alias("person2deletionDate")
          )
          .distinct()
    },
    "companyNumEmployees" -> Factor(OrganisationType, PersonWorkAtCompanyType) { case Seq(organisation, workAt) =>
      val company = organisation.where($"Type" === "Company").cache()
      frequency(
        company.as("Company").join(workAt.as("workAt"), $"workAt.CompanyId" === $"Company.id"),
        value = $"workAt.PersonId",
        by = Seq($"Company.id", $"Company.name")
      )
    },
    "people4Hops" -> Factor(PersonType, PlaceType, PersonKnowsPersonType) { case Seq(person, place, knows) =>
      val cities        = place.where($"type" === "City").cache()
      val allKnows      = undirectedKnows(knows).cache()
      val minSampleSize = 100.0

      val peopleInChinaSample = (relations: DataFrame) => {
        val peopleInChina = person
          .as("Person")
          .join(cities.as("City"), $"City.id" === $"Person.LocationCityId")
          .where($"City.PartOfPlaceId" === 1) // Country with ID 1 is China

        val count = peopleInChina.count()

        val curveFactor = 1e-3

        // sigmoid to select more samples for smaller scale factors
        val sampleSize = Math.min(count, Math.max(minSampleSize, count / (1 + Math.exp(count * curveFactor)) * 2))

        val sampleFraction = Math.min(sampleSize / count, 1.0)

        log.info(s"Factor people4Hops: using ${sampleSize} samples (${sampleFraction * 100}%)")

        peopleInChina
          .orderBy($"Person.id")
          .sample(sampleFraction, 42)
          .join(relations.alias("knows"), $"Person.id" === $"knows.Person1Id")
          .select($"knows.Person1Id".alias("Person1Id"), $"knows.Person2Id".alias("Person2Id"))
      }

      val personPairs = nHops(
          allKnows,
          n = 4,
          joinKeys = ("Person2Id", "Person1Id"),
          sample = Some(peopleInChinaSample)
        )
        .join(person.as("Person1"), $"Person1.id" === $"Person1Id")
        .join(person.as("Person2"), $"Person2.id" === $"Person1Id")
        .select(
          $"Person1Id",
          $"Person2Id",
          $"Person1.creationDate".as("Person1CreationDate"),
          $"Person1.deletionDate".as("Person1DeletionDate"),
          $"Person2.creationDate".as("Person2CreationDate"),
          $"Person2.deletionDate".as("Person2DeletionDate")
        )
        .orderBy($"Person1Id", $"Person2Id")

      val sampleFractionPersonPairs = Math.min(10000.0 / personPairs.count(), 1.0)
      personPairs.sample(sampleFractionPersonPairs, 42)
    },
    "people2Hops" -> Factor(PersonType, PlaceType, PersonKnowsPersonType) { case Seq(person, place, knows) =>
      val cities        = place.where($"type" === "City").cache()
      val allKnows      = undirectedKnows(knows).cache()
      val minSampleSize = 100.0

      val peopleInChinaSample = (relations: DataFrame) => {
        val peopleInChina = person
          .as("Person")
          .join(cities.as("City"), $"City.id" === $"Person.LocationCityId")
          .where($"City.PartOfPlaceId" === 1) // Country with ID 1 is China

        val count = peopleInChina.count()

        val curveFactor = 1e-3

        // sigmoid to select more samples for smaller scale factors
        val sampleSize = Math.min(count, Math.max(minSampleSize, count / (1 + Math.exp(count * curveFactor)) * 2))

        val sampleFraction = Math.min(sampleSize / count, 1.0)

        log.info(s"Factor people4Hops: using ${sampleSize} samples (${sampleFraction * 100}%)")

        peopleInChina
          .orderBy($"Person.id")
          .sample(sampleFraction, 42)
          .join(relations.alias("knows"), $"Person.id" === $"knows.Person1Id")
          .select($"knows.Person1Id".alias("Person1Id"), $"knows.Person2Id".alias("Person2Id"))
      }

      val personPairs = nHops(
          allKnows,
          n = 2,
          joinKeys = ("Person2Id", "Person1Id"),
          sample = Some(peopleInChinaSample)
        )
        .join(person.as("Person1"), $"Person1.id" === $"Person1Id")
        .join(person.as("Person2"), $"Person2.id" === $"Person1Id")
        .select(
          $"Person1Id",
          $"Person2Id",
          $"Person1.creationDate".as("Person1CreationDate"),
          $"Person1.deletionDate".as("Person1DeletionDate"),
          $"Person2.creationDate".as("Person2CreationDate"),
          $"Person2.deletionDate".as("Person2DeletionDate")
        )
        .orderBy($"Person1Id", $"Person2Id")

      val sampleFractionPersonPairs = Math.min(10000.0 / personPairs.count(), 1.0)
      personPairs.sample(sampleFractionPersonPairs, 42)
    },
    // -- interactive --
    // first names
    "personFirstNames" -> Factor(PersonType) { case Seq(person) =>
      frequency(person, value = $"id", by = Seq($"firstName"))
    },
    // friends
    "personNumFriendsOfFriendsOfFriends" -> Factor(PersonKnowsPersonType, PersonType) { case Seq(personKnowsPerson, person1) =>
      // direct friends
      val knows1 = person1
        .as("Person1")
        .join(undirectedKnows(personKnowsPerson).as("knows"), $"Person1.id" === $"knows.Person1Id", "leftouter")

      val personNumFriends = frequency(
        knows1,
        value = $"knows.Person2Id",
        by = Seq($"Person1.id", $"Person1.creationDate", $"Person1.deletionDate"),
        agg = count)
        .select($"Person1.id".as("Person1Id"), $"creationDate", $"deletionDate", $"frequency".as("numFriends"))

      // friends of friends
      val personFriendsOfFriends = personNumFriends.as("personNumFriends1")
        .join(undirectedKnows(personKnowsPerson).as("knows"), $"personNumFriends1.Person1Id" === $"knows.Person1Id", "leftouter")
        .join(personNumFriends.as("personNumFriends2"),       $"personNumFriends2.Person1Id" === $"knows.Person2Id", "leftouter")

      val personNumFriendsOfFriends = frequency(
        personFriendsOfFriends,
        value = $"personNumFriends2.numFriends",
        by = Seq($"personNumFriends1.Person1Id", $"personNumFriends1.creationDate", $"personNumFriends1.deletionDate", $"personNumFriends1.numFriends"),
        agg = sum
      ).select($"Person1Id", $"creationDate", $"deletionDate", $"numFriends", $"frequency".as("numFriendsOfFriends"))

      // friends of friends of friends
      val personFriendsOfFriendsOfFriends = personNumFriendsOfFriends.as("personNumFriendsOfFriends1")
        .join(undirectedKnows(personKnowsPerson).as("knows"),            $"personNumFriendsOfFriends1.Person1Id" === $"knows.Person1Id", "leftouter")
        .join(personNumFriendsOfFriends.as("personNumFriendsOfFriends2"),$"personNumFriendsOfFriends2.Person1Id" === $"knows.Person2Id", "leftouter")

      val personNumFriendsOfFriendsOfFriends = frequency(
        personFriendsOfFriendsOfFriends,
        value = $"personNumFriendsOfFriends2.numFriendsOfFriends",
        by = Seq($"personNumFriendsOfFriends1.Person1Id",
                 $"personNumFriendsOfFriends1.creationDate", $"personNumFriendsOfFriends1.deletionDate",
                 $"personNumFriendsOfFriends1.numFriends", $"personNumFriendsOfFriends1.numFriendsOfFriends"),
        agg = sum
      ).select($"Person1Id", $"creationDate", $"deletionDate", $"numFriends", $"numFriendsOfFriends", $"frequency".as("numFriendsOfFriendsOfFriends"))

      personNumFriendsOfFriendsOfFriends
    },
    // posts
    "personNumFriendOfFriendPosts" -> Factor(PersonType, PersonKnowsPersonType, PostType) { case Seq(person, personKnowsPerson, post) =>
      val personPosts = person
        .as("Person")
        .join(post.as("Post"), $"Post.CreatorPersonId" === $"Person.id", "leftouter")

      // direct posts
      val numPersonPosts = frequency(
        personPosts,
        value = $"Post.id",
        by = Seq($"Person.id", $"Person.creationDate", $"Person.deletionDate"),
        agg = count
      ).select($"Person.id".as("Person1Id"), $"creationDate", $"deletionDate", $"frequency".as("numDirectPosts"))

      // posts of friends
      val friendPosts = numPersonPosts.as("numPersonPosts1")
        .join(undirectedKnows(personKnowsPerson).as("knows"), $"numPersonPosts1.Person1Id" === $"knows.Person1Id", "leftouter")
        .join(numPersonPosts.as("numPersonPosts2"),           $"numPersonPosts2.Person1Id" === $"knows.Person2Id", "leftouter")

      val numFriendPosts = frequency(
        friendPosts,
        value = $"numPersonPosts2.numDirectPosts",
        by = Seq($"numPersonPosts1.Person1Id", $"numPersonPosts1.creationDate", $"numPersonPosts1.deletionDate", $"numPersonPosts1.numDirectPosts"),
        agg = sum
      ).select($"numPersonPosts1.Person1Id".as("Person1Id"), $"creationDate", $"deletionDate", $"numDirectPosts", $"frequency".as("numFriendPosts"))

      // posts of friends of friends
      val friendOfFriendPosts = numFriendPosts.as("numFriendPosts1")
        .join(undirectedKnows(personKnowsPerson).as("knows"), $"numFriendPosts1.Person1Id" === $"knows.Person1Id", "leftouter")
        .join(numFriendPosts.as("numFriendPosts2"),           $"numFriendPosts2.Person1Id" === $"knows.Person2Id", "leftouter")

      val numFriendOfFriendPosts = frequency(
        friendOfFriendPosts,
        value = $"numFriendPosts2.numFriendPosts",
        by = Seq($"numFriendPosts1.Person1Id", $"numFriendPosts1.creationDate", $"numFriendPosts1.deletionDate", $"numFriendPosts1.numDirectPosts", $"numFriendPosts1.numFriendPosts"),
        agg = sum
      ).select($"Person1Id", $"creationDate", $"deletionDate", $"numDirectPosts", $"numFriendPosts", $"frequency".as("numFriendOfFriendPosts"))

      numFriendOfFriendPosts
    },
    // comments
    "personNumFriendOfFriendComments" -> Factor(PersonType, PersonKnowsPersonType, CommentType) { case Seq(person, personKnowsPerson, comment) =>
      val personComments = person
        .as("Person")
        .join(comment.as("Comment"), $"Comment.CreatorPersonId" === $"Person.id", "leftouter")

      // direct comments
      val numPersonComments = frequency(
        personComments,
        value = $"Comment.id",
        by = Seq($"Person.id", $"Person.creationDate", $"Person.deletionDate"),
        agg = count
      ).select($"Person.id".as("Person1Id"), $"creationDate", $"deletionDate", $"frequency".as("numDirectComments"))

      // comments of friends
      val friendComments = numPersonComments.as("numPersonComments1")
        .join(undirectedKnows(personKnowsPerson).as("knows"), $"numPersonComments1.Person1Id" === $"knows.Person1Id", "leftouter")
        .join(numPersonComments.as("numPersonComments2"),     $"numPersonComments2.Person1Id" === $"knows.Person2Id", "leftouter")

      val numFriendComments = frequency(
        friendComments,
        value = $"numPersonComments2.numDirectComments",
        by = Seq($"numPersonComments1.Person1Id", $"numPersonComments1.creationDate", $"numPersonComments1.deletionDate", $"numPersonComments1.numDirectComments"),
        agg = sum
      ).select($"numPersonComments1.Person1Id".as("Person1Id"), $"creationDate", $"deletionDate", $"numDirectComments", $"frequency".as("numFriendComments"))

      // comments of friends of friends
      val friendOfFriendComments = numFriendComments.as("numFriendComments1")
        .join(undirectedKnows(personKnowsPerson).as("knows"), $"numFriendComments1.Person1Id" === $"knows.Person1Id", "leftouter")
        .join(numFriendComments.as("numFriendComments2"),     $"numFriendComments2.Person1Id" === $"knows.Person2Id", "leftouter")

      val numFriendOfFriendComments = frequency(
        friendOfFriendComments,
        value = $"numFriendComments2.numFriendComments",
        by = Seq($"numFriendComments1.Person1Id", $"numFriendComments1.creationDate", $"numFriendComments1.deletionDate", $"numFriendComments1.numDirectComments", $"numFriendComments1.numFriendComments"),
        agg = sum
      ).select($"Person1Id", $"creationDate", $"deletionDate", $"numDirectComments", $"numFriendComments", $"frequency".as("numFriendOfFriendComments"))

      numFriendOfFriendComments
    },
    // likes
    "personLikesNumMessages" -> Factor(PersonType, PersonLikesCommentType, PersonLikesPostType) { case Seq(person, personLikesComment, personLikesPost) =>
      val personLikesMessage =
        personLikesComment.select($"PersonId", $"CommentId".as("MessageId")) |+|
        personLikesPost.select($"PersonId", $"PostId".as("MessageId"))

      val messages = person
        .as("Person")
        .join(personLikesMessage.as("personLikesMessage"), $"personLikesMessage.PersonId" === $"Person.id", "leftouter")

      val personLikesNumMessages = frequency(
        messages,
        value = $"personLikesMessage.MessageId",
        by = Seq($"Person.id", $"Person.creationDate", $"Person.deletionDate"),
        agg = count
      ).select($"id".as("Person1Id"), $"creationDate", $"deletionDate", $"frequency")
      personLikesNumMessages
    },
    // tags
    "personNumFriendTags" -> Factor(PersonType, PersonHasInterestTagType, PersonKnowsPersonType) { case Seq(person, interest, personKnowsPerson) =>
      // direct tags
      val personComments = person
        .as("Person")
        .join(interest.as("interest"), $"interest.PersonId" === $"Person.id", "leftouter")

      val numPersonTags = frequency(
        personComments,
        value = $"TagId",
        by = Seq($"PersonId", $"Person.creationDate", $"Person.deletionDate"),
        agg = count
      ).select($"PersonId".as("Person1Id"), $"Person.creationDate", $"Person.deletionDate", $"frequency".as("numDirectTags"))

      // tags of friends
      val friendTags = numPersonTags.as("numPersonTags1")
        .join(undirectedKnows(personKnowsPerson).as("knows"), $"numPersonTags1.Person1Id" === $"knows.Person1Id", "leftouter")
        .join(numPersonTags.as("numPersonTags2"),             $"numPersonTags2.Person1Id" === $"knows.Person2Id", "leftouter")

      val numFriendTags = frequency(
        friendTags,
        value = $"numPersonTags2.numDirectTags",
        by = Seq($"numPersonTags1.Person1Id", $"numPersonTags1.creationDate", $"numPersonTags1.deletionDate", $"numPersonTags1.numDirectTags"),
        agg = sum
      ).select($"Person1Id", $"creationDate", $"deletionDate", $"numDirectTags", $"frequency".as("numFriendTags"))
      numFriendTags
    },
    // forums
    "personNumFriendOfFriendForums" -> Factor(PersonType, ForumHasMemberType, PersonKnowsPersonType) { case Seq(person, hasMember, personKnowsPerson) =>
      // direct forums
      val directForums = person.as("Person")
        .join(hasMember.as("hasMember"), $"hasMember.PersonId" === $"Person.id", "leftouter")

      val numForums = frequency(
        directForums,
        value = $"ForumId",
        by = Seq($"Person.id", $"Person.creationDate", $"Person.deletionDate"),
        agg = count
      ).select($"Person.id".as("Person1Id"), $"Person.creationDate", $"Person.deletionDate", $"frequency".as("numDirectForums"))

      val friendForums = numForums.as("numForums1")
        .join(undirectedKnows(personKnowsPerson).as("knows"), $"numForums1.Person1Id" === $"knows.Person1Id", "leftouter")
        .join(numForums.as("numForums2"),                     $"numForums2.Person1Id" === $"knows.Person2Id", "leftouter")

      // forums of friends
      val numFriendForums = frequency(
        friendForums,
        value = $"numForums2.numDirectForums",
        by = Seq($"numForums1.Person1Id", $"numForums1.creationDate", $"numForums1.deletionDate", $"numForums1.numDirectForums"),
        agg = sum
      ).select($"Person1Id", $"creationDate", $"deletionDate", $"numDirectForums", $"frequency".as("numFriendForums"))

      // forums of friends of friends
      val friendOfFriendForums = numFriendForums.as("numFriendForums1")
        .join(undirectedKnows(personKnowsPerson).as("knows"), $"numFriendForums1.Person1Id" === $"knows.Person1Id", "leftouter")
        .join(numFriendForums.as("numFriendForums2"),         $"numFriendForums2.Person1Id" === $"knows.Person2Id", "leftouter")

      val numFriendOfFriendForums = frequency(
        friendOfFriendForums,
        value = $"numFriendForums2.numFriendForums",
        by = Seq($"numFriendForums1.Person1Id", $"numFriendForums1.creationDate", $"numFriendForums1.deletionDate", $"numFriendForums1.numDirectForums", $"numFriendForums1.numFriendForums"),
        agg = sum
      ).select($"Person1Id", $"creationDate", $"deletionDate", $"numDirectForums", $"numFriendForums", $"frequency".as("numFriendOfFriendForums"))
      numFriendOfFriendForums
    },
    // companies
    "personNumFriendOfFriendCompanies" -> Factor(PersonType, PersonWorkAtCompanyType, PersonKnowsPersonType) { case Seq(person, workAt, personKnowsPerson) =>
      // direct companies
      val directCompanies = person.as("Person")
        .join(workAt.as("workAt"), $"workAt.PersonId" === $"Person.id", "leftouter")

      val numCompanies = frequency(
        directCompanies,
        value = $"CompanyId",
        by = Seq($"Person.id", $"Person.creationDate", $"Person.deletionDate"),
        agg = count
      ).select($"Person.id".as("Person1Id"), $"Person.creationDate", $"Person.deletionDate", $"frequency".as("numDirectCompanies"))

      val friendCompanies = numCompanies.as("numCompanies1")
        .join(undirectedKnows(personKnowsPerson).as("knows"), $"numCompanies1.Person1Id" === $"knows.Person1Id", "leftouter")
        .join(numCompanies.as("numCompanies2"),               $"numCompanies2.Person1Id" === $"knows.Person2Id", "leftouter")

      // companies of friends
      val numFriendCompanies = frequency(
        friendCompanies,
        value = $"numCompanies2.numDirectCompanies",
        by = Seq($"numCompanies1.Person1Id", $"numCompanies1.creationDate", $"numCompanies1.deletionDate", $"numCompanies1.numDirectCompanies"),
        agg = sum
      ).select($"Person1Id", $"creationDate", $"deletionDate", $"numDirectCompanies", $"frequency".as("numFriendCompanies"))

      // companies of friends of friends
      val friendOfFriendCompanies = numFriendCompanies.as("numFriendCompanies1")
        .join(undirectedKnows(personKnowsPerson).as("knows"), $"numFriendCompanies1.Person1Id" === $"knows.Person1Id", "leftouter")
        .join(numFriendCompanies.as("numFriendCompanies2"),   $"numFriendCompanies2.Person1Id" === $"knows.Person2Id", "leftouter")

      val numFriendOfFriendCompanies = frequency(
        friendOfFriendCompanies,
        value = $"numFriendCompanies2.numFriendCompanies",
        by = Seq(
          $"numFriendCompanies1.Person1Id", $"numFriendCompanies1.creationDate", $"numFriendCompanies1.deletionDate",
          $"numFriendCompanies1.numDirectCompanies", $"numFriendCompanies1.numFriendCompanies"
        ),
        agg = sum
      ).select($"Person1Id", $"creationDate", $"deletionDate", $"numDirectCompanies", $"numFriendCompanies", $"frequency".as("numFriendOfFriendCompanies"))

      numFriendOfFriendCompanies
    },
    "sameUniversityConnected" -> LargeFactor(PersonType, PersonKnowsPersonType, PersonStudyAtUniversityType) { case Seq(person, personKnowsPerson, studyAt) =>
      val s = spark
      import s.implicits._
      val vertices = person.select("id").rdd.map(row => (row.getAs[Long]("id"), ()))

      val edges = sameUniversityKnows(personKnowsPerson, studyAt).rdd.map(row =>
        graphx.Edge(row.getAs[Long]("person1Id"), row.getAs[Long]("person2Id"), ())
      )
      val graph = graphx.Graph(vertices, edges, ())
      val cc = graph.connectedComponents().vertices
        .toDF("PersonId", "Component")

      val counts = cc.groupBy("Component").agg(count("*").as("count"))

      cc.join(counts, Seq("Component")).select("PersonId", "Component", "count")

    },
    "personKnowsPersonConnected" -> LargeFactor(PersonType, PersonKnowsPersonType) { case Seq(person, personKnowsPerson) =>
      val s = spark
      import s.implicits._
      val vertices = person.select("id").rdd.map(row => (row.getAs[Long]("id"), ()))

      val edges = personKnowsPerson.rdd.map(row =>
        graphx.Edge(row.getAs[Long]("Person1Id"), row.getAs[Long]("Person2Id"), ())
      )
      val graph = graphx.Graph(vertices, edges, ())
      val cc = graph.connectedComponents().vertices
        .toDF("PersonId", "Component")

      val counts = cc.groupBy("Component").agg(count("*").as("count"))

      cc.join(counts, Seq("Component")).select("PersonId", "Component", "count")
    },
    "personKnowsPersonDays" -> LargeFactor(PersonKnowsPersonType) { case Seq(personKnowsPerson) =>
      personKnowsPerson.select($"Person1Id", $"Person2Id",
        date_trunc("day", $"creationDate").as("creationDay"),
        date_trunc("day", $"deletionDate").as("deletionDay")
      )
    },
    "personStudyAtUniversityDays" -> LargeFactor(PersonStudyAtUniversityType) { case Seq(personStudyAtUniversity) =>
      personStudyAtUniversity.select($"PersonId", $"UniversityId",
        date_trunc("day", $"creationDate").as("creationDay"),
        date_trunc("day", $"deletionDate").as("deletionDay")
      )
    },
    "personWorkAtCompanyDays" -> LargeFactor(PersonWorkAtCompanyType) { case Seq(personWorkAtCompany) =>
      personWorkAtCompany.select($"PersonId", $"CompanyId",
        date_trunc("day", $"creationDate").as("creationDay"),
        date_trunc("day", $"deletionDate").as("deletionDay")
      )
    },
    "personDays" -> LargeFactor(PersonType) { case Seq(person) =>
      person.select($"id",
        date_trunc("day", $"creationDate").as("creationDay"),
        date_trunc("day", $"deletionDate").as("deletionDay")
      )
    },
  )
}
