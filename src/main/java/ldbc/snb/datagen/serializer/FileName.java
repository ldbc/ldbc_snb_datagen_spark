package ldbc.snb.datagen.serializer;

public enum FileName {

    // static
    TAG("tag"),
    TAG_HASTYPE_TAGCLASS("tag_hasType_tagclass"),
    TAGCLASS("tagclass"),
    TAGCLASS_ISSUBCLASSOF_TAGCLASS("tagclass_isSubclassOf_tagclass"),
    PLACE("place"),
    PLACE_ISPARTOF_PLACE("place_isPartOf_place"),
    ORGANISATION("organisation"),
    ORGANISATION_ISLOCATEDIN_PLACE("organisation_isLocatedIn_place"),

    // dynamic activity
    FORUM("forum"),
    FORUM_CONTAINEROF_POST("forum_containerOf_post"),
    FORUM_HASMEMBER_PERSON("forum_hasMember_person"),
    FORUM_HASMODERATOR_PERSON("forum_hasModerator_person"),
    FORUM_HASTAG_TAG("forum_hasTag_tag"),
    PERSON_LIKES_POST("person_likes_post"),
    PERSON_LIKES_COMMENT("person_likes_comment"),
    POST("post"),
    POST_HASCREATOR_PERSON("post_hasCreator_person"),
    POST_HASTAG_TAG("post_hasTag_tag"),
    POST_ISLOCATEDIN_PLACE("post_isLocatedIn_place"),
    COMMENT("comment"),
    COMMENT_HASCREATOR_PERSON("comment_hasCreator_person"),
    COMMENT_HASTAG_TAG("comment_hasTag_tag"),
    COMMENT_ISLOCATEDIN_PLACE("comment_isLocatedIn_place"),
    COMMENT_REPLYOF_POST("comment_replyOf_post"),
    COMMENT_REPLYOF_COMMENT("comment_replyOf_comment"),

    // dynamic person
    PERSON("person"),
    PERSON_SPEAKS_LANGUAGE("person_speaks_language"),
    PERSON_EMAIL_EMAILADDRESS("person_email_emailaddress"),
    PERSON_ISLOCATEDIN_PLACE("person_isLocatedIn_place"),
    PERSON_HASINTEREST_TAG("person_hasInterest_tag"),
    PERSON_WORKAT_ORGANISATION("person_workAt_organisation"),
    PERSON_STUDYAT_ORGANISATION("person_studyAt_organisation"),
    PERSON_KNOWS_PERSON("person_knows_person"),

    ;

    private final String name;

    FileName(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }

}
