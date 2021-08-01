package ldbc.snb.datagen.factors

import ldbc.snb.datagen.model.Mode.Raw
import ldbc.snb.datagen.model.{Graph, graphs}
import ldbc.snb.datagen.syntax._
import org.apache.spark.sql.functions.{broadcast, count}
import org.apache.spark.sql.{Column, DataFrame}

object Factors {

  def frequency(df: DataFrame, value: Column, by: Seq[Column], sortBy: Seq[Column]) =
    df
      .groupBy(by: _*).agg(count(value).as("count"))
      .select(by :+ $"count": _*)
      .orderBy($"count".desc +: by: _*)


  def countryNumPersons(graph: Graph[Raw.type]): FactorTable[Raw.type] = {
    val places = graph.entities(graphs.Raw.entities.Place).cache()
    val cities = places.where($"type" === "City")
    val countries = places.where($"type" === "Country")

    val persons = graph.entities(graphs.Raw.entities.Person)
    val df = frequency(
      persons.as("Person")
        .join(broadcast(cities.as("City")), $"City.id" === $"Person.LocationCityId")
        .join(broadcast(countries.as("Country")), $"Country.id" === $"City.PartOfPlaceId"),
      value = $"Person.id",
      by = Seq($"Country.id", $"Country.name"),
      sortBy = Seq($"Country.id")
    )
    FactorTable[Raw.type](name="countryNumPersons", data=df, source=graph)
  }
}
