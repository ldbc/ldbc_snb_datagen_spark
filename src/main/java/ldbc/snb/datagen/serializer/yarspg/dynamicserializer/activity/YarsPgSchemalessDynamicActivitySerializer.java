package ldbc.snb.datagen.serializer.yarspg.dynamicserializer.activity;


import avro.shaded.com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.hadoop.writer.HdfsYarsPgWriter;
import ldbc.snb.datagen.serializer.DynamicActivitySerializer;
import ldbc.snb.datagen.serializer.FileName;
import ldbc.snb.datagen.serializer.yarspg.EdgeType;
import ldbc.snb.datagen.serializer.yarspg.Relationship;
import ldbc.snb.datagen.serializer.yarspg.Statement;
import ldbc.snb.datagen.serializer.yarspg.YarsPgSerializer;
import ldbc.snb.datagen.serializer.yarspg.property.PrimitiveType;

import java.util.List;

import static ldbc.snb.datagen.serializer.FileName.SOCIAL_NETWORK_ACTIVITY;

public class YarsPgSchemalessDynamicActivitySerializer extends DynamicActivitySerializer<HdfsYarsPgWriter> implements YarsPgSerializer {
    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(SOCIAL_NETWORK_ACTIVITY);
    }

    @Override
    public void writeFileHeaders() {
    }


    public void serialize(final Forum forum) {
        String nodeID = Statement.generateId("Forum" + forum.getId());

        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeNode(nodeID, (node) -> {
                    node.withNodeLabels("Forum")
                            .withProperties(properties -> {
                                properties.add("id", property ->
                                        property.generatePrimitive(PrimitiveType.STRING,
                                                Long.toString(forum.getId()))
                                );
                                properties.add("title", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, forum.getTitle())
                                );
                            });
                });

        String forumEdgeID = Statement.generateId("Forum" + forum.getId());
        for (Integer tagId : forum.getTags()) {
            String tagEdgeID = Statement.generateId("Tag" + tagId);
            writers.get(SOCIAL_NETWORK_ACTIVITY)
                    .writeEdge(EdgeType.DIRECTED, (edge) -> {
                        edge.as(forumEdgeID, Relationship.HAS_TAG.toString(), tagEdgeID);
                    });
        }

        String personEdgeID = Statement.generateId("Person" + forum.getModerator().getAccountId());
        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeEdge(EdgeType.DIRECTED, (edge) -> {
                    edge.as(forumEdgeID, Relationship.HAS_MODERATOR.toString(), personEdgeID);
                });
    }

    public void serialize(final Post post) {
        String postEdgeID = Statement.generateId("Post" + post.getMessageId());
        String nodeID = Statement.generateId("Post" + post.getMessageId());

        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeNode(nodeID, (node) -> {
                    node.withNodeLabels("Post")
                            .withProperties(properties -> {
                                properties.add("language", property ->
                                        property.generatePrimitive(PrimitiveType.STRING,
                                                Dictionaries.languages.getLanguageName(post.getLanguage()))
                                );
                                properties.add("imageFile", property ->
                                        property.generatePrimitive(PrimitiveType.NULL, "null")
                                );
                            });
                });

        for (Integer tagId : post.getTags()) {
            String tagEdgeID = Statement.generateId("Tag" + tagId);

            writers.get(SOCIAL_NETWORK_ACTIVITY)
                    .writeEdge(EdgeType.DIRECTED, (edge) -> {
                        edge.as(postEdgeID, Relationship.HAS_TAG.toString(), tagEdgeID);
                    });
        }

        String forumEdgeID = Statement.generateId("Post" + post.getForumId());
        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    edge.as(forumEdgeID, Relationship.CONTAINER_OF.toString(), postEdgeID);
                });
    }

    public void serialize(final Comment comment) {
        String commentEdgeID = Statement.generateId("Comment" + comment.getRootPostId());
        String messageEdgeID = Statement.generateId("Message" + comment.getMessageId());

        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeEdge(EdgeType.DIRECTED, (edge) -> {
                    edge.as(commentEdgeID, Relationship.IS_REPLY_OF.toString(), messageEdgeID);
                });
    }

    public void serialize(final Photo photo) {
    }

    public void serialize(final ForumMembership membership) {
        String forumEdgeID = Statement.generateId("Forum" + membership.getForumId());
        String personEdgeID = Statement.generateId("Person" + membership.getForumId());

        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeEdge(EdgeType.DIRECTED, (edge) -> {
                    edge.as(forumEdgeID, Relationship.HAS_MEMBER.toString(), personEdgeID)
                            .withProperties(properties -> {
                                properties.add("creationDate", property ->
                                        property.generatePrimitive(PrimitiveType.DATE_TIME, formatDateTime(membership.getCreationDate())));
                            });
                });
    }

    public void serialize(final Like like) {
        String personEdgeID = Statement.generateId("Person" + like.getPerson());
        String messageEdgeID = Statement.generateId("Message" + like.getMessageId());

        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeEdge(EdgeType.DIRECTED, (edge) -> {
                    edge.as(personEdgeID, Relationship.LIKES.toString(), messageEdgeID)
                            .withProperties(properties -> {
                                properties.add("creationDate", property ->
                                        property.generatePrimitive(PrimitiveType.DATE_TIME, formatDateTime(like.getCreationDate())));
                            });
                });
    }

    @Override
    protected boolean isDynamic() {
        return true;
    }

}

