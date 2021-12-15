package ldbc.snb.datagen.factors

import ldbc.snb.datagen.factors.io.FactorTableSink
import ldbc.snb.datagen.io.graphs.GraphSource
import ldbc.snb.datagen.model
import ldbc.snb.datagen.model.EntityType
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.transformation.transform.ConvertDates
import ldbc.snb.datagen.util.{DatagenStage, Logging}
import org.apache.spark.sql.functions.{broadcast, count, date_trunc, sum}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

case class Factor(requiredEntities: EntityType*)(f: Seq[DataFrame] => DataFrame) extends (Seq[DataFrame] => DataFrame) {
  override def apply(v1: Seq[DataFrame]): DataFrame = f(v1)
}

object FactorGenerationStage extends DatagenStage with Logging {

  case class Args(outputDir: String = "out", irFormat: String = "parquet")

  def run(args: Args)(implicit spark: SparkSession): Unit = {
    import ldbc.snb.datagen.factors.io.instances._
    import ldbc.snb.datagen.io.Reader.ops._
    import ldbc.snb.datagen.io.Writer.ops._
    import ldbc.snb.datagen.io.instances._

    GraphSource(model.graphs.Raw.graphDef, args.outputDir, args.irFormat).read
      .pipe(ConvertDates.transform)
      .pipe(g =>
        rawFactors.map { case (name, calc) =>
          val resolvedEntities = calc.requiredEntities.foldLeft(Seq.empty[DataFrame])((args, et) => args :+ g.entities(et))
          FactorTable(name, calc(resolvedEntities), g)
        }
      )
      .foreach(_.write(FactorTableSink(args.outputDir)))
  }

  private def frequency(df: DataFrame, value: Column, by: Seq[Column], agg: Column => Column = count) =
    df
      .groupBy(by: _*)
      .agg(agg(value).as("frequency"))
      .select(by :+ $"frequency": _*)
      .orderBy($"frequency".desc +: by.map(_.asc): _*)

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
    "countryNumMessages" -> Factor(CommentType, PostType) { case Seq(comments, posts) =>
      frequency(
        comments.select($"id", $"LocationCountryId") |+| posts.select($"id", $"LocationCountryId"),
        value = $"id",
        by = Seq($"LocationCountryId")
      )
    },
    "cityPairsNumFriends" -> Factor(PersonKnowsPersonType, PersonType, PlaceType) { case Seq(personKnowsPerson, persons, places) =>
      val cities = places.where($"type" === "City").cache()

      frequency(
        personKnowsPerson
          .alias("Knows")
          .join(persons.cache().as("Person1"), $"Person1.id" === $"Knows.Person1Id")
          .join(cities.cache().as("City1"), $"City1.id" === "Person1.LocationCityId")
          .join(persons.as("Person2"), $"Person2.id" === $"Knows.Person2Id")
          .join(cities.as("City2"), $"City2.id" === "Person2.LocationCityId")
          .where($"City1.id" < $"City2.id"),
        value = $"*",
        by = Seq($"City1.id", $"City2.id", $"City1.name", $"City2.name")
      ).select(
        $"City1.id".alias("city1Id"),
        $"City2.id".alias("city2Id"),
        $"City1.name".alias("city1Name"),
        $"City2.name".alias("city2Name"),
        $"frequency"
      )
    },
    "countryPairsNumFriends" -> Factor(PersonKnowsPersonType, PersonType, PlaceType) { case Seq(personKnowsPerson, persons, places) =>
      val cities    = places.where($"type" === "City").cache()
      val countries = places.where($"type" === "Country").cache()

      frequency(
        personKnowsPerson
          .alias("Knows")
          .join(persons.cache().as("Person1"), $"Person1.id" === $"Knows.Person1Id")
          .join(cities.cache().as("City1"), $"City1.id" === "Person1.LocationCityId")
          .join(countries.cache().as("Country1"), $"Country1.id" === "City1.PartOfPlaceId")
          .join(persons.as("Person2"), $"Person2.id" === $"Knows.Person2Id")
          .join(cities.as("City2"), $"City2.id" === "Person2.LocationCityId")
          .join(countries.as("Country2"), $"Country2.id" === "City2.PartOfPlaceId")
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
    "messageCreationDays" -> Factor(CommentType, PostType) { case Seq(comments, posts) =>
      (comments.select($"creationDate") |+| posts.select($"creationDate"))
        .select(date_trunc("day", $"creationDate").as("creationDay"))
        .distinct()
    },
    "messageLengths" -> Factor(CommentType, PostType) { case Seq(comments, posts) =>
      frequency(
        comments.select($"id", $"length") |+| posts.select($"id", $"length"),
        value = $"id",
        by = Seq($"length")
      )
    },
    "messageTags" -> Factor(CommentHasTagType, PostHasTagType, TagType) { case Seq(commentHasTag, postHasTag, tag) =>
      messageTags(commentHasTag, postHasTag, tag).cache()
    },
    "messageTagClasses" -> Factor(CommentHasTagType, PostHasTagType, TagType, TagClassType) { case Seq(commentHasTag, postHasTag, tag, tagClass) =>
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
    "personNumFriends" -> Factor(PersonKnowsPersonType) { case Seq(knows) =>
      frequency(knows, value = $"Person2Id", by = Seq($"Person1Id"))
    },
    "postLanguages" -> Factor(PostType) { case Seq(post) =>
      frequency(post.where($"language".isNotNull), value = $"id", by = Seq($"language"))
    },
    "tagClassNumTags" -> Factor(TagClassType, TagType) { case Seq(tagClass, tag) =>
      frequency(
        tag.as("Tag").join(tagClass.as("TagClass"), $"Tag.TypeTagClassId" === $"TagClass.id"),
        value = $"Tag.id",
        by = Seq($"TagClass.id", $"TagClass.name")
      )
    },
    "companiesNumEmployees" -> Factor(OrganisationType, PersonWorkAtCompanyType) { case Seq(organisation, workAt) =>
      val company = organisation.where($"Type" === "Company").cache()
      frequency(
        company.as("Company").join(workAt.as("WorkAt"), $"WorkAt.CompanyId" === $"Company.id"),
        value = $"WorkAt.PersonId",
        by = Seq($"Company.id", $"Company.name")
      )
    }
  )
}
