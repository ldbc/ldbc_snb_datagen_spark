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
        String leftEdgeID = Statement.generateId("Forum" + forum.getId());
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

        for (Integer tagId : forum.getTags()) {
            String rightEdgeID = Statement.generateId("Tag" + tagId);
            writers.get(SOCIAL_NETWORK_ACTIVITY)
                    .writeEdge(EdgeType.DIRECTED, (edge) -> {
                        edge.as(leftEdgeID, Relationship.HAS_TAG.toString(), rightEdgeID);
                    });
        }
    }

    public void serialize(final Post post) {
        String leftEdgeID = Statement.generateId("Post" + post.getMessageId());
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
            String rightEdgeID = Statement.generateId("Tag" + tagId);

            writers.get(SOCIAL_NETWORK_ACTIVITY)
                    .writeEdge(EdgeType.DIRECTED, (edge) -> {
                        edge.as(leftEdgeID, Relationship.HAS_TAG.toString(), rightEdgeID);
                    });
        }
    }

    public void serialize(final Comment comment) {
        String leftEdgeID = Statement.generateId("Comment" + comment.getRootPostId());
        String rightEdgeID = Statement.generateId("Comment" + comment.getMessageId());

        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeEdge(EdgeType.DIRECTED, (edge) -> {
                    edge.as(leftEdgeID, Relationship.IS_REPLY_OF.toString(), rightEdgeID);
                });
    }

    public void serialize(final Photo photo) {
    }

    public void serialize(final ForumMembership membership) {
        String leftEdgeID = Statement.generateId("ForumMembership" + membership.getForumId());
        String rightEdgeID = Statement.generateId("ForumMembership" + membership.getForumId());

        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeEdge(EdgeType.DIRECTED, (edge) -> {
                    edge.as(leftEdgeID, Relationship.HAS_MEMBER.toString(), rightEdgeID)
                            .withProperties(properties -> {
                                properties.add("creationDate", property ->
                                        property.generatePrimitive(PrimitiveType.DATE_TIME, formatDateTime(membership.getCreationDate())));
                            });
                });
    }

    public void serialize(final Like like) {
        String leftEdgeID = Statement.generateId("Like" + like.getPerson());
        String rightEdgeID = Statement.generateId("Like" + like.getMessageId());

        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeEdge(EdgeType.DIRECTED, (edge) -> {
                    edge.as(leftEdgeID, Relationship.LIKES.toString(), rightEdgeID)
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

