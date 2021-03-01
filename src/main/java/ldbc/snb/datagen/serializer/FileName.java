package ldbc.snb.datagen.serializer;

public enum FileName {

    // static
    TAG("Tag"),
    TAG_HASTYPE_TAGCLASS("Tag_hasType_TagClass"),
    TAGCLASS("TagClass"),
    TAGCLASS_ISSUBCLASSOF_TAGCLASS("TagClass_isSubclassOf_TagClass"),
    PLACE("Place"),
    PLACE_ISPARTOF_PLACE("Place_isPartOf_Place"),
    ORGANISATION("Organisation"),
    ORGANISATION_ISLOCATEDIN_PLACE("Organisation_isLocatedIn_Place"),

    // dynamic activity
    FORUM("Forum"),
    FORUM_CONTAINEROF_POST("Forum_containerOf_Post"),
    FORUM_HASMEMBER_PERSON("Forum_hasMember_Person"),
    FORUM_HASMODERATOR_PERSON("Forum_hasModerator_Person"),
    FORUM_HASTAG_TAG("Forum_hasTag_Tag"),
    PERSON_LIKES_POST("Person_likes_Post"),
    PERSON_LIKES_COMMENT("Person_likes_Comment"),
    POST("Post"),
    POST_HASCREATOR_PERSON("Post_hasCreator_Person"),
    POST_HASTAG_TAG("Post_hasTag_Tag"),
    POST_ISLOCATEDIN_COUNTRY("Post_isLocatedIn_Country"),
    COMMENT("Comment"),
    COMMENT_HASCREATOR_PERSON("Comment_hasCreator_Person"),
    COMMENT_HASTAG_TAG("Comment_hasTag_Tag"),
    COMMENT_ISLOCATEDIN_COUNTRY("Comment_isLocatedIn_Country"),
    COMMENT_REPLYOF_POST("Comment_replyOf_Post"),
    COMMENT_REPLYOF_COMMENT("Comment_replyOf_Comment"),

    // dynamic person
    PERSON("Person"),
    PERSON_SPEAKS_LANGUAGE("Person_speaks_language"),
    PERSON_EMAIL_EMAILADDRESS("Person_email_emailaddress"),
    PERSON_ISLOCATEDIN_CITY("Person_isLocatedIn_City"),
    PERSON_HASINTEREST_TAG("Person_hasInterest_Tag"),
    PERSON_WORKAT_COMPANY("Person_workAt_Company"),
    PERSON_STUDYAT_UNIVERSITY("Person_studyAt_University"),
    PERSON_STUDYAT_ORGANISATION("Person_studyAt_Organization"),
    PERSON_KNOWS_PERSON("Person_knows_Person"),


    // single file for each
    SOCIAL_NETWORK_STATIC("social_network_static"),
    SOCIAL_NETWORK_ACTIVITY("social_network_activity"),
    SOCIAL_NETWORK_PERSON("social_network_person"),
    ;

    private final String name;

    FileName(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }

}
