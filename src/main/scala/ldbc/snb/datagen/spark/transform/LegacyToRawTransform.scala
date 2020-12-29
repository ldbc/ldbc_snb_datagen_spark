package ldbc.snb.datagen.spark.transform

import ldbc.snb.datagen.model.Cardinality.{NN, OneN}
import ldbc.snb.datagen.model.EntityType.{Attr, Edge, Node}
import ldbc.snb.datagen.model.{Graph, legacy}
import ldbc.snb.datagen.spark.model.DataFrameGraph
import ldbc.snb.datagen.spark.sql._
import ldbc.snb.datagen.syntax._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ByteType

object LegacyToRawTransform extends Transform {
  override def transform(input: DataFrameGraph): DataFrameGraph = {
    // as a rule of thumb try to cache every dataset that is used more than once
    // and is close to the leaf

    val legacyPersons = input.entities(Node("Person")).cache()
    val legacyActivities = input.entities(Node("Activity"))

    val temporalAttributes = Seq(
      $"creationDate", // map to date
      $"deletionDate", // map to date
      $"isExplicitlyDeleted".as("explicitlyDeleted")
    )

    val formatIP = (ip: Column) => {
      val getByte = (address: Column, pos: Int) =>
        pmod(shiftLeft(address, pos).cast(ByteType), lit(256))

      val address = ip.getField("ip")
      format_string(
        "%d.%d.%d.%d",
        getByte(address, legacy.IP.BYTE1_SHIFT_POSITION),
        getByte(address, legacy.IP.BYTE2_SHIFT_POSITION),
        getByte(address, legacy.IP.BYTE3_SHIFT_POSITION),
        getByte(address, legacy.IP.BYTE4_SHIFT_POSITION)
      )
    }

    val cached = (df: DataFrame) => df.cache()

    val personalWall = legacyActivities
      .select(explode($"wall").as("wall"))
      .select($"wall.*")

    val groupWall = legacyActivities
      .select(explode($"groups").as("group"))
      .select(explode($"group").as("wall"))
      .select($"wall.*")

    val textWalls = (personalWall |+| groupWall).cache()

    val photoWall = legacyActivities
      .select(explode($"albums").as("album"))
      .select($"album.*")
      .cache()

    val forumFromWall = (wall: DataFrame) => wall
      .select($"_1.*")
      .cache()

    val forumMembershipFromWall = (wall: DataFrame) => wall
      .select(explode($"_2").as("fm"))
      .select($"fm.*")

    val treeFromWall = (wall: DataFrame) => wall
      .select(explode($"_3").as("pt"))
      .select($"pt.*")

    val postFromTree = (pt: DataFrame) => pt.select($"_1.*")

    val photoFromTree = (pt: DataFrame) => pt.select($"_1.*")

    val likesFromTree = (pt: DataFrame) => pt
      .select(explode($"_2").as("_2"))
      .select($"_2.*")

    val commentTreeFromTree = (pt: DataFrame) => pt
      .select(explode($"_3").as("_3"))
      .select($"_3.*")

    val commentFromCommentTree = (ct: DataFrame) => ct
      .select($"_1.*")

    val commentFromWall = cached compose commentFromCommentTree compose commentTreeFromTree compose treeFromWall

    val comment = commentFromWall(textWalls)
      .select(
        temporalAttributes ++ Seq(
          $"messageId".as("id"),
          formatIP($"ipAddress").as("locationIP"),
          $"browserId".as("browserUsed"), // join small dict
          $"content".as("content"),
          length($"content").as("length")
        )
      )

    val commentHasCreatorPerson = commentFromWall(textWalls)
      .select(
        temporalAttributes ++ Seq(
          $"messageId".as("Comment.id"),
          $"author.accountId".as("Person.id")
        )
      )

    val commentHasTagTag = commentFromWall(textWalls)
      .select(
        temporalAttributes ++ Seq(
          $"messageId".as("Comment.id"),
          explode($"tags").as("Tag.id")
        )
      )

    val commentIsLocatedInPlace = commentFromWall(textWalls)
      .select(
        temporalAttributes ++ Seq(
          $"messageId".as("Comment.id"),
          $"countryId".as("Place.id")
        )
      )

    val (commentReplyOfPost, commentReplyOfComment) = commentFromWall(textWalls)
      .partition($"rootPostId" === $"parentMessageId")
      .let {
        case (post, comment) => (
          post.select(temporalAttributes ++ Seq(
            $"messageId".as("Comment.id"),
            $"parentMessageId".as("ParentPost.id")
          )),
          comment.select(temporalAttributes ++ Seq(
            $"messageId".as("Comment.id"),
            $"parentMessageId".as("ParentComment.id")
          ))
        )
      }

    val forum = (forumFromWall(textWalls) |+| forumFromWall(photoWall))
      .select(
        temporalAttributes ++ Seq(
          $"id",
          $"title",
          $"forumType".as("type") // map to string
        )
      )

    val forumContainerOfPost = postFromTree(treeFromWall(textWalls))
      .select(
        temporalAttributes ++ Seq(
          $"forumId".as("Forum.id"),
          $"messageId".as("Post.Id")
        )
      )

    val forumHasMemberPerson = (forumMembershipFromWall(textWalls) |+| forumMembershipFromWall(photoWall))
      .select(
        temporalAttributes ++ Seq(
          $"forumId".as("Forum.id"),
          $"person.accountId".as("Person.id"),
          $"forumType".as("type") // map to string
        )
      )

    val forumHasModeratorPerson = (forumFromWall(textWalls) |+| forumFromWall(photoWall))
      .select(
        temporalAttributes ++ Seq(
          $"id".as("Forum.id"),
          $"moderator.accountId".as("Person.id")
        )
      )

    val forumHasTagTag = (forumFromWall(textWalls) |+| forumFromWall(photoWall))
      .select(
        temporalAttributes ++ Seq(
          $"id".as("Forum.id"),
          explode($"tags").as("Tag.id")
        )
      )

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
          )
      )

    val personEmailEmailAddress = legacyPersons
      .select(
        temporalAttributes ++
          Seq(
            $"accountId".as("Person.id"),
            explode($"emails").as("email")
          )
      )

    val personSpeaksLanguage = legacyPersons
      .select(
        temporalAttributes ++
          Seq(
            $"accountId".as("Person.id"),
            explode($"languages").as("language")
          )
      )

    val personHasInterestTag = legacyPersons
      .select(
        temporalAttributes ++ Seq(
          $"accountId".as("Person.id"),
          explode($"interests").as("Tag.id")
        )
      )

    val personIsLocatedInPlace = legacyPersons
      .select(
        temporalAttributes ++ Seq(
          $"accountId".as("Person.id"),
          $"cityId".as("Place.id")
        )
      )

    val personKnowsPerson = legacyPersons
      .select(
        Seq(
          $"creationDate",
          $"deletionDate",
          $"isExplicitlyDeleted",
          $"accountId",
          explode($"knows").as("know")
        )
      )
      .select(
        temporalAttributes ++ Seq(
          $"accountId".as("Person1.id"),
          $"know.to.accountId".as("Person2.id"),
          $"know.weight".as("weight")
        )
      )
      .where($"`Person1.id`" < $"`Person2.id`")

    val personLikesComment = likesFromTree(commentTreeFromTree(treeFromWall(textWalls)))
      .select(temporalAttributes ++ Seq(
        $"person".as("Person.id"),
        $"messageId".as("Comment.id")
      ))

    val likesFromWall = likesFromTree compose treeFromWall

    val personLikesPost = (likesFromWall(textWalls) |+| likesFromWall(photoWall))
        .select(temporalAttributes ++ Seq(
          $"person".as("Person.id"),
          $"messageId".as("Post.id")
        ))

    val personStudyAtOrganisation = legacyPersons
      .select(
        temporalAttributes ++ Seq(
          $"accountId".as("Person.id"),
          $"universityLocationId".as("Organisation.id"), // join small dictionary
          $"classYear" // format year
        )
      )
      .where($"`Organisation.id`" =!= -1)

    val personWorkAtOrganisation = legacyPersons
      .select(
        temporalAttributes ++ Seq(
          $"accountId".as("Person.id"),
          explode($"companies").as(Seq("Organisation.id", "workFrom"))
        )
      )

    val textPost = postFromTree(treeFromWall(textWalls))
      .select(
        temporalAttributes ++ Seq(
          $"messageId".as("id"),
          lit(null).as("imageFile"),
          formatIP($"ipAddress").as("locationIP"),
          $"browserId".as("browserUsed"), // join small dict
          $"language".as("language"), // join small dict
          $"content".as("content"),
          length($"content").as("length"),
          $"forumId".as("Forum.id")
        )
      )

    val postLocationFromPost = (post: DataFrame) => post
      .select(
        temporalAttributes ++ Seq(
          $"messageId".as("Post.id"),
          $"countryId".as("Place.id")
        )
      )

    val postCreationFromPost = (post: DataFrame) => post
      .select(
        temporalAttributes ++ Seq(
          $"messageId".as("Post.id"),
          $"author.accountId".as("Person.id")
        )
      )

    val postTagFromPost = (post: DataFrame) => post
      .select(
        temporalAttributes ++ Seq(
          $"messageId".as("Post.id"),
          explode($"tags").as("Tag.id")
        )
      )

    val postFromWall = cached compose postFromTree compose treeFromWall

    val postHasCreatorPerson =
      postCreationFromPost(postFromWall(textWalls)) |+|
        postCreationFromPost(postFromWall(photoWall))

    val postHasTagTag =
      postTagFromPost(postFromWall(textWalls)) |+|
        postTagFromPost(postFromWall(photoWall))

    val postIsLocatedInPlace =
      postLocationFromPost(postFromWall(textWalls)) |+|
        postLocationFromPost(postFromWall(photoWall))

    val photoPost = photoFromTree(treeFromWall(photoWall))
      .select(
        temporalAttributes ++ Seq(
          $"messageId".as("id"),
          $"content".as("imageFile"),
          formatIP($"ipAddress").as("locationIP"),
          $"browserId".as("browserUsed"), // join small dict
          lit(null).as("language"), // join small dict
          lit(null).as("content"),
          lit(0).as("length"),
          $"forumId".as("Forum.id")
        )
      )

    val post = textPost |+| photoPost

    Graph("Raw", Map(
      Node("Comment") -> comment,
      Edge("HasCreator", "Comment", "Person", OneN) -> commentHasCreatorPerson,
      Edge("HasTag", "Comment", "Tag", NN) -> commentHasTagTag,
      Edge("IsLocatedIn", "Comment", "Place", OneN) -> commentIsLocatedInPlace,
      Edge("ReplyOf", "Comment", "Comment", OneN) -> commentReplyOfComment,
      Edge("ReplyOf", "Comment", "Post", OneN) -> commentReplyOfPost,
      Node("Forum") -> forum,
      Edge("ContainerOf", "Forum", "Post", NN) -> forumContainerOfPost, // cardinality?
      Edge("HasMember", "Forum", "Person", NN) -> forumHasMemberPerson,
      Edge("HasModerator", "Forum", "Person", OneN) -> forumHasModeratorPerson, // MaybeN?
      Edge("HasTag", "Forum", "Tag", NN) -> forumHasTagTag,
      Node("Person") -> person,
      Attr("Email", "Person", "EmailAddress") -> personEmailEmailAddress,
      Edge("HasInterest", "Person", "Tag", NN) -> personHasInterestTag,
      Edge("IsLocatedIn", "Person", "Place", OneN) -> personIsLocatedInPlace,
      Edge("Knows", "Person", "Person", NN) -> personKnowsPerson,
      Edge("Likes", "Person", "Comment", NN) -> personLikesComment,
      Edge("Likes", "Person", "Post", NN) -> personLikesPost,
      Attr("Speaks", "Person", "Language") -> personSpeaksLanguage,
      Edge("StudyAt", "Person", "Organisation", OneN) -> personStudyAtOrganisation,
      Edge("WorkAt", "Person", "Organisation", NN) -> personWorkAtOrganisation,
      Node("Post") -> post,
      Edge("HasCreator", "Post", "Person", OneN) -> postHasCreatorPerson,
      Edge("HasTag", "Post", "Tag", NN) -> postHasTagTag,
      Edge("IsLocatedIn", "Post", "Place", OneN) -> postIsLocatedInPlace
    ))
  }
}
