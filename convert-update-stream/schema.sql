DROP TABLE IF EXISTS Comment;
DROP TABLE IF EXISTS Comment_hasTag_Tag;
DROP TABLE IF EXISTS Forum;
DROP TABLE IF EXISTS Forum_hasMember_Person;
DROP TABLE IF EXISTS Forum_hasTag_Tag;
DROP TABLE IF EXISTS Person;
DROP TABLE IF EXISTS Person_hasInterest_Tag;
DROP TABLE IF EXISTS Person_knows_Person;
DROP TABLE IF EXISTS Person_likes_Comment;
DROP TABLE IF EXISTS Person_likes_Post;
DROP TABLE IF EXISTS Person_studyAt_University;
DROP TABLE IF EXISTS Person_workAt_Company;
DROP TABLE IF EXISTS Post;
DROP TABLE IF EXISTS Post_hasTag_Tag;

-- merged
DROP TABLE IF EXISTS Comment_Update;
DROP TABLE IF EXISTS Forum_Update;
DROP TABLE IF EXISTS Forum_hasMember_Person_Update;
DROP TABLE IF EXISTS Person_Update;
DROP TABLE IF EXISTS Person_knows_Person_Update;
DROP TABLE IF EXISTS Person_likes_Comment_Update;
DROP TABLE IF EXISTS Person_likes_Post_Update;
DROP TABLE IF EXISTS Post_Update;

CREATE TABLE Comment (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    locationIP varchar(40) NOT NULL,
    browserUsed varchar(40) NOT NULL,
    content varchar(2000) NOT NULL,
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    LocationCountryId bigint NOT NULL,
    ParentPostId bigint,
    ParentCommentId bigint
);

CREATE TABLE Forum (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    title varchar(256) NOT NULL,
    ModeratorPersonId bigint -- can be null as its cardinality is 0..1
);

CREATE TABLE Post (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    imageFile varchar(40),
    locationIP varchar(40) NOT NULL,
    browserUsed varchar(40) NOT NULL,
    language varchar(40),
    content varchar(2000),
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    ContainerForumId bigint NOT NULL,
    LocationCountryId bigint NOT NULL
);

CREATE TABLE Person (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    firstName varchar(40) NOT NULL,
    lastName varchar(40) NOT NULL,
    gender varchar(40) NOT NULL,
    birthday date NOT NULL,
    locationIP varchar(40) NOT NULL,
    browserUsed varchar(40) NOT NULL,
    LocationCityId bigint NOT NULL,
    speaks varchar(640) NOT NULL,
    email varchar(8192) NOT NULL
);

-- edges
CREATE TABLE Comment_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    CommentId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE TABLE Post_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    PostId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE TABLE Forum_hasMember_Person (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    PersonId bigint NOT NULL
);

CREATE TABLE Forum_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE TABLE Person_hasInterest_Tag (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE TABLE Person_likes_Comment (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CommentId bigint NOT NULL
);

CREATE TABLE Person_likes_Post (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    PostId bigint NOT NULL
);

CREATE TABLE Person_studyAt_University (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    UniversityId bigint NOT NULL,
    classYear int NOT NULL
);

CREATE TABLE Person_workAt_Company (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CompanyId bigint NOT NULL,
    workFrom int NOT NULL
);

CREATE TABLE Person_knows_Person (
    creationDate timestamp with time zone NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL
);

-- Updates

-- INS7
CREATE TABLE Comment_Update (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    locationIP varchar(40) NOT NULL,
    browserUsed varchar(40) NOT NULL,
    content varchar(2000) NOT NULL,
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    LocationCountryId bigint NOT NULL,
    ParentPostId bigint,
    ParentCommentId bigint,
    TagIds string
);

-- INS4
CREATE TABLE Forum_Update (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    title varchar(256) NOT NULL,
    ModeratorPersonId bigint,
    TagIds string
);

-- INS6
CREATE TABLE Post_Update (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    imageFile varchar(40),
    locationIP varchar(40) NOT NULL,
    browserUsed varchar(40) NOT NULL,
    language varchar(40),
    content varchar(2000),
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    ContainerForumId bigint NOT NULL,
    LocationCountryId bigint NOT NULL,
    TagIds string
);

-- INS1
CREATE TABLE Person_Update (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    firstName varchar(40) NOT NULL,
    lastName varchar(40) NOT NULL,
    gender varchar(40) NOT NULL,
    birthday date NOT NULL,
    locationIP varchar(40) NOT NULL,
    browserUsed varchar(40) NOT NULL,
    LocationCityId bigint NOT NULL,
    speaks varchar(640) NOT NULL,
    email varchar(8192) NOT NULL,
    studyAt string,
    workAt string
);

-- INS5
CREATE TABLE Forum_hasMember_Person_Update (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    PersonId bigint NOT NULL
);

-- INS3
CREATE TABLE Person_likes_Comment_Update (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CommentId bigint NOT NULL
);

-- INS2
CREATE TABLE Person_likes_Post_Update (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    PostId bigint NOT NULL
);

-- INS8
CREATE TABLE Person_knows_Person_Update (
    creationDate timestamp with time zone NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL
);
