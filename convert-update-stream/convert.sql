INSERT INTO Comment_Update
    SELECT
        Comment.creationDate,
        Comment.id,
        Comment.locationIP,
        Comment.browserUsed,
        Comment.content,
        Comment.length,
        Comment.CreatorPersonId,
        Comment.LocationCountryId,
        Comment.ParentPostId,
        Comment.ParentCommentId,
        string_agg(DISTINCT Comment_hasTag_Tag.TagId, ';') AS tagIds
    FROM Comment
    JOIN Comment_hasTag_Tag
      ON Comment_hasTag_Tag.CommentId = Comment.id
    GROUP BY ALL
    ORDER BY Comment.creationDate
;

INSERT INTO Post_Update
    SELECT
        Post.creationDate,
        Post.id,
        Post.imageFile,
        Post.locationIP,
        Post.browserUsed,
        Post.language,
        Post.content,
        Post.length,
        Post.CreatorPersonId,
        Post.ContainerForumId,
        Post.LocationCountryId,
        string_agg(DISTINCT Post_hasTag_Tag.TagId, ';') AS tagIds
    FROM Post
    JOIN Post_hasTag_Tag
      ON Post_hasTag_Tag.PostId = Post.id
    GROUP BY ALL
    ORDER BY Post.creationDate
;

INSERT INTO Person_Update
    SELECT
        Person.creationDate,
        Person.id,
        Person.firstName,
        Person.lastName,
        Person.gender,
        Person.birthday,
        Person.locationIP,
        Person.browserUsed,
        Person.LocationCityId,
        Person.speaks,
        Person.email,
        string_agg(DISTINCT Person_hasInterest_Tag.TagId, ';') AS tagIds,
        string_agg(DISTINCT Person_studyAt_University.UniversityId || ',' || Person_studyAt_University.classYear, ';') AS studyAt,
        string_agg(DISTINCT Person_workAt_Company.CompanyId || ',' || Person_workAt_Company.workFrom, ';') AS workAt
    FROM Person
    JOIN Person_studyAt_University
      ON Person_studyAt_University.PersonId = Person.id
    JOIN Person_workAt_Company
      ON Person_workAt_Company.PersonId = Person.id
    JOIN Person_hasInterest_Tag
      ON Person_hasInterest_Tag.PersonId = Person.id
    GROUP BY ALL
    ORDER BY Person.creationDate
;

INSERT INTO Person_knows_Person_Update
    SELECT *
    FROM Person_knows_Person
    ORDER BY creationDate
;

INSERT INTO Person_likes_Comment_Update
    SELECT *
    FROM Person_likes_Comment
    ORDER BY creationDate
;

INSERT INTO Person_likes_Post_Update
    SELECT *
    FROM Person_likes_Post
    ORDER BY creationDate
;

INSERT INTO Forum_hasMember_Person_Update
    SELECT *
    FROM Forum_hasMember_Person
    ORDER BY creationDate
;
