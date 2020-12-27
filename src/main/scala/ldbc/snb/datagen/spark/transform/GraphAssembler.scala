package ldbc.snb.datagen.spark.transform

import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.generators.GenActivity
import ldbc.snb.datagen.model.Cardinality.{N1, NN}
import ldbc.snb.datagen.model.EntityType.{Attr, Edge, Node}
import ldbc.snb.datagen.model.{Graph, legacy}
import ldbc.snb.datagen.spark.util.Utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ByteType

object GraphAssembler {

  import ldbc.snb.datagen.entities.EntityConversion.ops._
  import ldbc.snb.datagen.entities.EntityConversionInstances._

  val RawPersonColumns = Seq(
    "creationDate", "deletionDate", "explicitlyDeleted",
    "id", "locationIP"
  )

  def apply(personEntities: RDD[Person], activityEntities: RDD[(Long, Array[GenActivity])])(implicit spark: SparkSession) = {

    import frameless._
    import ldbc.snb.datagen.spark.encoder._

    val legacyPersons = spark.createDataset(personEntities.mapPartitions(_.map(_.repr)))

    val legacyActivities = spark
      .createDataset(activityEntities.mapValues(_.map(_.repr)))

    val temporalAttributes = Seq(
      $"creationDate", // map to date
      $"deletionDate", // map to date
      $"isExplicitlyDeleted".as("explicitlyDeleted")
    )

    def formatIP(ip: Column): Column = {
      def getByte(address: Column, pos: Int) = pmod(shiftLeft(address, pos).cast(ByteType), lit(256))
      val address = ip.getField("ip")
      format_string(
        "%d.%d.%d.%d",
        getByte(address, legacy.IP.BYTE1_SHIFT_POSITION),
        getByte(address, legacy.IP.BYTE2_SHIFT_POSITION),
        getByte(address, legacy.IP.BYTE3_SHIFT_POSITION),
        getByte(address, legacy.IP.BYTE4_SHIFT_POSITION)
      )
    }

    val person = legacyPersons
      .select(
        temporalAttributes ++
          Seq(
            $"accountId".as("id"),
            $"firstName",
            $"lastName",
            $"gender", // map to string
            $"birthday", // map to date
            formatIP($"ipAddress").as("locationIP"),
            $"browserId".as("browserUsed") // join small dictionary
          ): _*
      )

    val personEmailEmailAddress = legacyPersons
      .select(
        temporalAttributes ++
          Seq(
            $"accountId".as("Person.id"),
            explode($"emails").as("email")
          ): _*
      )

    val personSpeaksLanguage = legacyPersons
      .select(
        temporalAttributes ++
          Seq(
            $"accountId".as("Person.id"),
            explode($"languages").as("language")
          ): _*
      )

    val personHasInterestTag = legacyPersons
      .select(
        temporalAttributes ++ Seq(
          $"accountId".as("Person.id"),
          explode($"interests").as("Tag.id")
        ): _*
      )

    val personIsLocatedInPlace = legacyPersons
      .select(
        temporalAttributes ++ Seq(
          $"accountId".as("Person.id"),
          $"cityId".as("Place.id")
        ): _*
      )

    val personKnowsPerson = legacyPersons.
      select(
        Seq(
          $"creationDate",
          $"deletionDate",
          $"isExplicitlyDeleted",
          $"accountId",
          explode($"knows").as("know")
        ): _*
      )
      .select(
        temporalAttributes ++ Seq(
          $"accountId".as("Person1.id"),
          $"know.to.accountId".as("Person2.id"),
          $"know.weight".as("weight")
        ): _*
      )
      .where($"`Person1.id`" < $"`Person2.id`")

    //    val personLikesComment = ???
    //
    //    val personLikesPost = ???

    val personStudyAtOrganisation = legacyPersons.
      select(
        temporalAttributes ++ Seq(
          $"accountId".as("Person.id"),
          $"universityLocationId".as("Organisation.id"), // join small dictionary
          $"classYear" // format year
        ): _*
      )
      .where($"`Organisation.id`".isNotNull) // -1 to null

    val personWorkAtOrganisation = legacyPersons
      .select(
        temporalAttributes ++ Seq(
          $"accountId".as("Person.id"),
          explode($"companies").as(Seq("Organisation.id", "workFrom"))
        ): _*
      )

    //    val forum = legacyActivities.
    //      select(
    //        explode($"_2.wall._1.creationDate").as($"creationDate")
    //
    //      )

    Graph("Raw", Map(
      Node("Person") -> person,
      Attr("Email", "Person", "EmailAddress") -> personEmailEmailAddress,
      Attr("Speaks", "Person", "Language") -> personSpeaksLanguage,
      Edge("HasInterest", "Person", "Tag", NN) -> personHasInterestTag,
      Edge("LocatedIn", "Person", "Place", N1) -> personIsLocatedInPlace,
      Edge("Knows", "Person", "Person", NN) -> personKnowsPerson,
      Edge("StudyAt", "Person", "Organisation", N1) -> personStudyAtOrganisation,
      Edge("WorkAt", "Person", "Organisation", NN) -> personWorkAtOrganisation
    ))
  }
}
