package ldbc.snb.datagen.factors

import ldbc.snb.datagen.factors.io.FactorTableSink
import ldbc.snb.datagen.io.graphs.GraphSource
import ldbc.snb.datagen.model
import ldbc.snb.datagen.model.EntityType
import ldbc.snb.datagen.model.Mode.Raw
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.transformation.transform.ConvertDates
import ldbc.snb.datagen.util.{DatagenStage, Logging}
import org.apache.spark.sql.functions.{broadcast, col, count, date_trunc, expr, floor, lit, sum}
import org.apache.spark.sql.{Column, DataFrame, functions}
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
      .foreach(_.write(FactorTableSink(args.outputDir, overwrite = args.force)))
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

  import model.raw._

  private val rawFactors = Map(
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

      val chinesePeopleSample = (relations: DataFrame) => {
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
          .sample(sampleFraction, 42)
          .join(relations.alias("knows"), $"Person.id" === $"knows.Person1Id")
          .select($"knows.Person1Id".alias("Person1Id"), $"knows.Person2Id".alias("Person2Id"))
      }

      val personPairs = nHops(
          allKnows,
          n = 4,
          joinKeys = ("Person2Id", "Person1Id"),
          sample = Some(chinesePeopleSample)
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

      val sampleFractionPersonPairs = Math.min(10000.0 / personPairs.count(), 1.0)
      personPairs.sample(sampleFractionPersonPairs, 42)
    },
    "people2Hops" -> Factor(PersonType, PlaceType, PersonKnowsPersonType) { case Seq(person, place, knows) =>
      val cities        = place.where($"type" === "City").cache()
      val allKnows      = undirectedKnows(knows).cache()
      val minSampleSize = 100.0

      val chinesePeopleSample = (relations: DataFrame) => {
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
          .sample(sampleFraction, 42)
          .join(relations.alias("knows"), $"Person.id" === $"knows.Person1Id")
          .select($"knows.Person1Id".alias("Person1Id"), $"knows.Person2Id".alias("Person2Id"))
      }

      val personPairs = nHops(
          allKnows,
          n = 2,
          joinKeys = ("Person2Id", "Person1Id"),
          sample = Some(chinesePeopleSample)
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

      val sampleFractionPersonPairs = Math.min(10000.0 / personPairs.count(), 1.0)
      personPairs.sample(sampleFractionPersonPairs, 42)
    },
    "sameUniversityKnows" -> LargeFactor(PersonKnowsPersonType, PersonStudyAtUniversityType) { case Seq(personKnowsPerson, studyAt) =>
      val size = Math.max(Math.ceil(personKnowsPerson.rdd.getNumPartitions / 10).toInt, 1)
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
        .coalesce(size)
    },
    // -- interactive --
    // first names
    "personFirstNames" -> Factor(PersonType) { case Seq(person) =>
      frequency(
        person,
        value = $"id",
        by = Seq($"firstName")
      )
    },
    // friends
    "personNumFriendsOfFriends" -> Factor(PersonKnowsPersonType, PersonType) { case Seq(personKnowsPerson, person1) =>
      val foaf = person1
        .as("Person1")
        .join(undirectedKnows(personKnowsPerson).as("knows1"), $"Person1.id" === $"knows1.Person1Id", "leftouter")
        .join(undirectedKnows(personKnowsPerson).as("knows2"), $"knows1.Person2Id" === $"knows2.Person1Id", "leftouter")
      frequency(foaf, value = $"knows2.Person2Id", by = Seq($"Person1.id", $"Person1.creationDate", $"Person1.deletionDate"))
    },
    // posts
    "personNumPosts" -> Factor(PersonType, PostType) { case Seq(person, post) =>
      val posts = person
        .as("Person")
        .join(post.as("post"), $"post.CreatorPersonId" === $"Person.id", "leftouter")
      frequency(posts, value = $"post.id", by = Seq($"Person.id"))
    },
    "personNumFriendPosts" -> Factor(PersonType, PersonKnowsPersonType, PostType) { case Seq(person, personKnowsPerson, post) =>
      val posts = person
        .as("Person")
        .join(undirectedKnows(personKnowsPerson).as("knows1"), $"Person.id" === $"knows1.Person1Id", "leftouter")
        .join(post.as("post"), $"post.CreatorPersonId" === $"knows1.Person2Id", "leftouter")
      frequency(posts, value = $"post.id", by = Seq($"Person.id"))
    },
    "personNumFriendOfFriendPosts" -> Factor(PersonType, PersonKnowsPersonType, PostType) { case Seq(person, personKnowsPerson, post) =>
      val posts = person
        .as("Person")
        .join(undirectedKnows(personKnowsPerson).as("knows1"), $"Person.id" === $"knows1.Person1Id", "leftouter")
        .join(undirectedKnows(personKnowsPerson).as("knows2"), $"knows1.Person2Id" === $"knows2.Person1Id", "leftouter")
        .join(post.as("post"), $"post.CreatorPersonId" === $"knows1.Person2Id", "leftouter")
      frequency(posts, value = $"post.id", by = Seq($"Person.id"))
    },
    // comments
    "personNumComments" -> Factor(PersonType, PostType) { case Seq(person, comment) =>
      val comments = person
        .as("Person")
        .join(comment.as("comment"), $"comment.CreatorPersonId" === $"Person.id", "leftouter")
      frequency(comments, value = $"comment.id", by = Seq($"Person.id"))
    },
    "personNumFriendComments" -> Factor(PersonType, PersonKnowsPersonType, CommentType) { case Seq(person, personKnowsPerson, comment) =>
      val comments = person
        .as("Person")
        .join(undirectedKnows(personKnowsPerson).as("knows1"), $"Person.id" === $"knows1.Person1Id", "leftouter")
        .join(comment.as("comment"), $"comment.CreatorPersonId" === $"knows1.Person2Id", "leftouter")
      frequency(comments, value = $"comment.id", by = Seq($"Person.id"))
    },
    // likes
    "personLikesNumMessages" -> Factor(PersonType, PersonLikesCommentType, PersonLikesPostType) { case Seq(person, personLikesComment, personLikesPost) =>
      val personLikesMessage =
        personLikesComment.select($"PersonId", $"CommentId".as("MessageId")) |+|
          personLikesPost.select($"PersonId", $"PostId".as("MessageId"))
      val messages = person
        .as("Person")
        .join(personLikesMessage.as("personLikesMessage"), $"personLikesMessage.PersonId" === $"Person.id", "leftouter")
      frequency(messages, value = $"personLikesMessage.MessageId", by = Seq($"Person.id"))
    },
    // tags
    "personNumTags" -> Factor(PersonHasInterestTagType) { case Seq(interest) =>
      frequency(
        interest,
        value = $"TagId",
        by = Seq($"personId")
      )
    },
    "personNumFriendTags" -> Factor(PersonHasInterestTagType, PersonKnowsPersonType) { case Seq(interest, personKnowsPerson) =>
      frequency(
        undirectedKnows(personKnowsPerson).as("knows")
          .join(interest, $"personId" === $"knows.Person2Id", "leftouter"),
        value = $"TagId",
        by = Seq($"knows.Person1Id")
      )
    },
    // forums
    "personNumForums" -> Factor(ForumHasMemberType) { case Seq(hasMember) =>
      frequency(
        hasMember,
        value = $"ForumId",
        by = Seq($"PersonId")
      )
    },
    "personNumFriendForums" -> Factor(ForumHasMemberType, PersonKnowsPersonType) { case Seq(hasMember, personKnowsPerson) =>
      frequency(
        undirectedKnows(personKnowsPerson).as("knows")
          .join(hasMember, $"PersonId" === $"knows.Person2Id", "leftouter"),
        value = $"ForumId",
        by = Seq($"knows.Person1Id")
      )
    },
    "personNumFriendOfFriendForums" -> Factor(ForumHasMemberType, PersonKnowsPersonType) { case Seq(hasMember, personKnowsPerson) =>
      frequency(
        undirectedKnows(personKnowsPerson).as("knows1")
          .join(undirectedKnows(personKnowsPerson).as("knows2"), $"knows1.Person2Id" === $"knows2.Person1Id", "leftouter")
          .join(hasMember, $"PersonId" === $"knows2.Person2Id", "leftouter"),
        value = $"ForumId",
        by = Seq($"knows1.Person1Id")
      )
    },
    // companies
    "personNumCompanies" -> Factor(PersonWorkAtCompanyType) { case Seq(workAt) =>
      frequency(
        workAt,
        value = $"CompanyId",
        by = Seq($"PersonId")
      )
    },
    "personNumFriendCompanies" -> Factor(PersonWorkAtCompanyType, PersonKnowsPersonType) { case Seq(workAt, personKnowsPerson) =>
      frequency(
        undirectedKnows(personKnowsPerson).as("knows")
          .join(workAt, $"PersonId" === $"knows.Person2Id", "leftouter"),
        value = $"CompanyId",
        by = Seq($"knows.Person1Id")
      )
    },
    "personNumFriendOfFriendCompanies" -> Factor(PersonWorkAtCompanyType, PersonKnowsPersonType) { case Seq(workAt, personKnowsPerson) =>
      frequency(
        undirectedKnows(personKnowsPerson).as("knows1")
          .join(undirectedKnows(personKnowsPerson).as("knows2"), $"knows1.Person2Id" === $"knows2.Person1Id", "leftouter")
          .join(workAt, $"PersonId" === $"knows2.Person2Id", "leftouter"),
        value = $"CompanyId",
        by = Seq($"knows1.Person1Id")
      )
    },
  )
}
