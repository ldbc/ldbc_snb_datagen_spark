package ldbc.snb.datagen.factors

import ldbc.snb.datagen.factors.io.FactorTableSink
import ldbc.snb.datagen.io.graphs.GraphSource
import ldbc.snb.datagen.model.{Graph, Mode, graphs}
import ldbc.snb.datagen.{SparkApp, model}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.Logging
import org.apache.spark.sql.functions.{broadcast, count, date_trunc}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

case class RawFactor(private val f: Graph[Mode.Raw.type] => DataFrame) extends (Graph[Mode.Raw.type] => DataFrame) {
  override def apply(v1: Graph[Mode.Raw.type]): DataFrame = f(v1)
}

object FactorGenerationStage extends SparkApp with Logging {
  override def appName: String = "LDBC SNB Datagen for Spark: Factor Generation Stage"

  case class Args(outputDir: String = "out")

  def run(args: Args)(implicit spark: SparkSession): Unit = {
    import ldbc.snb.datagen.factors.io.instances._
    import ldbc.snb.datagen.io.Reader.ops._
    import ldbc.snb.datagen.io.Writer.ops._
    import ldbc.snb.datagen.io.instances._

    GraphSource(model.graphs.Raw.graphDef, args.outputDir, "csv")
      .read
      .pipe(g => rawFactors.map { case (name, calc) => FactorTable(name, calc(g), g) })
      .foreach(_.write(FactorTableSink(args.outputDir)))
  }

  private def frequency(df: DataFrame, value: Column, by: Seq[Column]) =
    df
      .groupBy(by: _*).agg(count(value).as("count"))
      .select(by :+ $"count": _*)
      .orderBy($"count".desc +: by.map(_.asc): _*)

  private val rawFactors = Map(
    "countryNumPersons" -> RawFactor { graph =>
      val places = graph.entities(graphs.Raw.entities.Place).cache()
      val cities = places.where($"type" === "City")
      val countries = places.where($"type" === "Country")

      val persons = graph.entities(graphs.Raw.entities.Person)
      frequency(
        persons.as("Person")
          .join(broadcast(cities.as("City")), $"City.id" === $"Person.LocationCityId")
          .join(broadcast(countries.as("Country")), $"Country.id" === $"City.PartOfPlaceId"),
        value = $"Person.id",
        by = Seq($"Country.id", $"Country.name")
      )
    },
    "countryNumMessages" -> RawFactor { graph =>
      val comments = graph.entities(graphs.Raw.entities.Comment)
      val posts = graph.entities(graphs.Raw.entities.Post)
      frequency(
        comments.select($"id", $"LocationCountryId") |+| posts.select($"id", $"LocationCountryId"),
        value = $"id",
        by = Seq($"LocationCountryId")
      )
    },
    "cityPairsNumFriends" -> RawFactor { graph =>
      val personKnowsPerson = graph.entities(graphs.Raw.entities.PersonKnowsPerson)
      val persons = graph.entities(graphs.Raw.entities.Person).cache()

      val places = graph.entities(graphs.Raw.entities.Place)
      val cities = places.where($"type" === "City").cache()

      frequency(
        personKnowsPerson.alias("Knows")
          .join(persons.as("Person1"), $"Person1.id" === $"Knows.Person1Id")
          .join(cities.as("City1"), $"City1.id" === "Person1.LocationCityId")
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
        $"count"
      )
    },
    "countryPairsNumFriends" -> RawFactor { graph =>
      val personKnowsPerson = graph.entities(graphs.Raw.entities.PersonKnowsPerson)
      val persons = graph.entities(graphs.Raw.entities.Person).cache()

      val places = graph.entities(graphs.Raw.entities.Place)
      val cities = places.where($"type" === "City").cache()
      val countries = places.where($"type" === "Country").cache()

      frequency(
        personKnowsPerson.alias("Knows")
          .join(persons.as("Person1"), $"Person1.id" === $"Knows.Person1Id")
          .join(cities.as("City1"), $"City1.id" === "Person1.LocationCityId")
          .join(countries.as("Country1"), $"Country1.id" === "City1.PartOfPlaceId")
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
        $"count"
      )
    },
    "messageCreationDays" -> RawFactor { graph =>
      val comments = graph.entities(graphs.Raw.entities.Comment)
      val posts = graph.entities(graphs.Raw.entities.Post)
      (comments.select($"creationDate") |+| posts.select($"creationDate"))
        .select(date_trunc("day", $"creationDate").as("creationDay"))
        .distinct()
    },
    "messageLengths" -> RawFactor { graph =>
      val comments = graph.entities(graphs.Raw.entities.Comment)
      val posts = graph.entities(graphs.Raw.entities.Post)
      frequency(
        comments.select($"id", $"length") |+| posts.select($"id", $"length"),
        value = $"id",
        by = Seq($"length")
      )
    }
  )
}


