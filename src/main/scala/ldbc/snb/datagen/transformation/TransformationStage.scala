package ldbc.snb.datagen.transformation

import ldbc.snb.datagen.SparkApp
import ldbc.snb.datagen.transformation.model.Cardinality.{NN, OneN}
import ldbc.snb.datagen.transformation.model.EntityType.{Edge, Node}
import ldbc.snb.datagen.transformation.model.{BatchedEntity, Graph, GraphDef, Mode}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.transformation.io._
import ldbc.snb.datagen.transformation.transform.{ExplodeAttrs, ExplodeEdges, RawToBiTransform, RawToInteractiveTransform}
import ldbc.snb.datagen.util.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import shapeless._

object TransformationStage extends SparkApp with Logging {
  override def appName: String = "LDBC SNB Datagen for Spark: TransformationStage"

  case class Args(
    outputDir: String = "out",
    explodeEdges: Boolean = false,
    explodeAttrs: Boolean = false,
    keepImplicitDeletes: Boolean = false,
    simulationStart: Long = 0,
    simulationEnd: Long = 0,
    mode: Mode = Mode.Raw,
    format: String = "csv",
    formatOptions: Map[String, String] = Map.empty
  )

  val inputGraphDefinition = GraphDef(
    isAttrExploded = false,
    isEdgesExploded = false,
    Mode.Raw,
    Map(
      Node("Organisation", isStatic = true) -> Some(
        "`id` INT, `type` STRING, `name` STRING, `url` STRING, `LocationPlaceId` INT"
      ),
      Node("Place", isStatic = true) -> Some(
        "`id` INT, `name` STRING, `url` STRING, `type` STRING, `PartOfPlaceId` INT"
      ),
      Node("Tag", isStatic = true) -> Some(
        "`id` INT, `name` STRING, `url` STRING, `TypeTagClassId` INT"
      ),
      Node("TagClass", isStatic = true) -> Some(
        "`id` INT, `name` STRING, `url` STRING, `SubclassOfTagClassId` INT"
      ),
      Node("Comment") -> Some(
        "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `id` BIGINT, `locationIP` STRING, `browserUsed` STRING, `content` STRING, `length` INT, `CreatorPersonId` BIGINT, `LocationCountryId` INT, `ParentPostId` BIGINT, `ParentCommentId` BIGINT"
      ),
      Edge("HasTag", "Comment", "Tag", NN) -> Some(
        "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `CommentId` BIGINT, `TagId` INT"
      ),
      Node("Forum") -> Some(
        "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `id` BIGINT, `title` STRING, `ModeratorPersonId` BIGINT"
      ),
      Edge("HasMember", "Forum", "Person", NN) -> Some(
        "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `ForumId` BIGINT, `PersonId` BIGINT"
      ),
      Edge("HasTag", "Forum", "Tag", NN) -> Some(
        "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `ForumId` BIGINT, `TagId` INT"
      ),
      Node("Person") -> Some(
        "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `id` BIGINT, `firstName` STRING, `lastName` STRING, `gender` STRING, `birthday` DATE, `locationIP` STRING, `browserUsed` STRING, `LocationCityId` INT, `language` STRING, `email` STRING"
      ),
      Edge("HasInterest", "Person", "Tag", NN) -> Some(
        "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `PersonId` BIGINT, `TagId` INT"
      ),
      Edge("Knows", "Person", "Person", NN) -> Some(
        "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `Person1Id` BIGINT, `Person2Id` BIGINT"
      ),
      Edge("Likes", "Person", "Comment", NN) -> Some(
        "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `PersonId` BIGINT, `CommentId` BIGINT"
      ),
      Edge("Likes", "Person", "Post", NN) -> Some(
        "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `PersonId` BIGINT, `PostId` BIGINT"
      ),
      Edge("StudyAt", "Person", "University", OneN) -> Some(
        "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `PersonId` BIGINT, `UniversityId` INT, `classYear` INT"
      ),
      Edge("WorkAt", "Person", "Company", NN) -> Some(
        "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `PersonId` BIGINT, `CompanyId` INT, `workFrom` INT"
      ),
      Node("Post") -> Some(
        "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `id` BIGINT, `imageFile` STRING, `locationIP` STRING, `browserUsed` STRING, `language` STRING, `content` STRING, `length` INT, `CreatorPersonId` BIGINT, `ContainerForumId` BIGINT, `LocationCountryId` INT"
      ),
      Edge("HasTag", "Post", "Tag", NN) -> Some(
        "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `PostId` BIGINT, `TagId` INT"
      )
    )
  )

  def run(args: Args)(implicit spark: SparkSession) = {
    object write extends Poly1 {
      implicit def caseSimple[M <: Mode](implicit ev: M#Layout[DataFrame] =:= DataFrame) = at[Graph[M, DataFrame]](g =>
        GraphWriter[M, DataFrame].write(g, args.outputDir, new WriterFormatOptions(args.format, g.mode, args.formatOptions))
      )

      implicit def caseBatched[M <: Mode](implicit ev: M#Layout[DataFrame] =:= BatchedEntity[DataFrame]) = at[Graph[M, DataFrame]](g =>
        GraphWriter[M, DataFrame].write(g, args.outputDir, new WriterFormatOptions(args.format, g.mode, args.formatOptions))
      )
    }

    type OutputTypes = Graph[Mode.Raw.type, DataFrame] :+:
      Graph[Mode.Interactive, DataFrame] :+:
      Graph[Mode.BI, DataFrame] :+:
      CNil

    GraphReader[Mode.Raw.type, DataFrame]
      .read(inputGraphDefinition, args.outputDir, new ReaderFormatOptions("csv", Mode.Raw))
      .pipeFoldLeft(args.explodeAttrs.fork)((graph, _: Unit) => ExplodeAttrs.transform(graph))
      .pipeFoldLeft(args.explodeEdges.fork)((graph, _: Unit) => ExplodeEdges.transform(graph))
      .pipe[OutputTypes] {
        g =>
          args.mode match {
            case bi@Mode.BI(_, _) => Inr(Inr(Inl(RawToBiTransform(bi, args.simulationStart, args.simulationEnd, args.keepImplicitDeletes).transform(g))))
            case interactive@Mode.Interactive(_) => Inr(Inl(RawToInteractiveTransform(interactive, args.simulationStart, args.simulationEnd).transform(g)))
            case Mode.Raw => Inl(g)
          }
      }
      .pipe(_.map(write))
    ()
  }
}
