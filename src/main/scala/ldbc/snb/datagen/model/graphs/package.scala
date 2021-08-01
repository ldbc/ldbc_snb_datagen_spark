package ldbc.snb.datagen.model

package object graphs {
  import EntityType._
  import Cardinality._

  object Raw {
    object entities {
      val Organisation = Node("Organisation", isStatic = true)
      val Place = Node("Place", isStatic = true)
      val Tag = Node("Tag", isStatic = true)
      val TagClass = Node("TagClass", isStatic = true)
      val Comment = Node("Comment")
      val CommentHasTag = Edge("HasTag", "Comment", "Tag", NN)
      val Forum = Node("Forum")
      val ForumHasMember = Edge("HasMember", "Forum", "Person", NN)
      val ForumHasTag = Edge("HasTag", "Forum", "Tag", NN)
      val Person = Node("Person")
      val PersonHasInterest = Edge("HasInterest", "Person", "Tag", NN)
      val PersonKnowsPerson = Edge("Knows", "Person", "Person", NN)
      val PersonLikesComment = Edge("Likes", "Person", "Comment", NN)
      val PersonLikesPost = Edge("Likes", "Person", "Post", NN)
      val PersonStudyAtUniversity = Edge("StudyAt", "Person", "University", OneN)
      val PersonWorkAtCompany = Edge("WorkAt", "Person", "Company", NN)
      val Post = Node("Post")
      val PostHasTag = Edge("HasTag", "Post", "Tag", NN)
    }

    import entities._

    val graphDef = GraphDef(
      isAttrExploded = false,
      isEdgesExploded = false,
      Mode.Raw,
      Map(
        Organisation -> Some(
          "`id` INT, `type` STRING, `name` STRING, `url` STRING, `LocationPlaceId` INT"
        ),
        Place -> Some(
          "`id` INT, `name` STRING, `url` STRING, `type` STRING, `PartOfPlaceId` INT"
        ),
        Tag -> Some(
          "`id` INT, `name` STRING, `url` STRING, `TypeTagClassId` INT"
        ),
        TagClass -> Some(
          "`id` INT, `name` STRING, `url` STRING, `SubclassOfTagClassId` INT"
        ),
        Comment -> Some(
          "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `id` BIGINT, `locationIP` STRING, `browserUsed` STRING, `content` STRING, `length` INT, `CreatorPersonId` BIGINT, `LocationCountryId` INT, `ParentPostId` BIGINT, `ParentCommentId` BIGINT"
        ),
        CommentHasTag -> Some(
          "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `CommentId` BIGINT, `TagId` INT"
        ),
        Forum -> Some(
          "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `id` BIGINT, `title` STRING, `ModeratorPersonId` BIGINT"
        ),
        ForumHasMember -> Some(
          "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `ForumId` BIGINT, `PersonId` BIGINT"
        ),
        ForumHasTag -> Some(
          "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `ForumId` BIGINT, `TagId` INT"
        ),
        Person -> Some(
          "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `id` BIGINT, `firstName` STRING, `lastName` STRING, `gender` STRING, `birthday` DATE, `locationIP` STRING, `browserUsed` STRING, `LocationCityId` INT, `language` STRING, `email` STRING"
        ),
        PersonHasInterest -> Some(
          "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `PersonId` BIGINT, `TagId` INT"
        ),
        PersonKnowsPerson -> Some(
          "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `Person1Id` BIGINT, `Person2Id` BIGINT"
        ),
        PersonLikesComment -> Some(
          "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `PersonId` BIGINT, `CommentId` BIGINT"
        ),
        PersonLikesPost -> Some(
          "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `PersonId` BIGINT, `PostId` BIGINT"
        ),
        PersonStudyAtUniversity -> Some(
          "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `PersonId` BIGINT, `UniversityId` INT, `classYear` INT"
        ),
        PersonWorkAtCompany -> Some(
          "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `PersonId` BIGINT, `CompanyId` INT, `workFrom` INT"
        ),
        Post -> Some(
          "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `explicitlyDeleted` BOOLEAN, `id` BIGINT, `imageFile` STRING, `locationIP` STRING, `browserUsed` STRING, `language` STRING, `content` STRING, `length` INT, `CreatorPersonId` BIGINT, `ContainerForumId` BIGINT, `LocationCountryId` INT"
        ),
        PostHasTag -> Some(
          "`creationDate` TIMESTAMP, `deletionDate` TIMESTAMP, `PostId` BIGINT, `TagId` INT"
        )
      )
    )
  }
}
