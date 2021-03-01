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
import ldbc.snb.datagen.serializer.yarspg.*;
import ldbc.snb.datagen.serializer.yarspg.property.PrimitiveType;

import java.util.List;

import static ldbc.snb.datagen.serializer.FileName.*;

public class YarsPgCanonicalDynamicActivitySerializer extends DynamicActivitySerializer<HdfsYarsPgWriter> implements YarsPgSerializer {
    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(SOCIAL_NETWORK_ACTIVITY);
    }

    @Override
    protected void addition() {
        writers.get(SOCIAL_NETWORK_ACTIVITY).setCanonical(true);
    }

    @Override
    public void writeFileHeaders() {
        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeHeader(Header.NODE_SCHEMA.toString());
        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeHeader(Header.EDGE_SCHEMA.toString());
        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeHeader(Header.NODES.toString());
        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeHeader(Header.EDGES.toString());
    }


    public void serialize(final Forum forum) {
        String leftSchemaEdgeID = Statement.generateId("S_Forum" + forum.getId());
        String leftEdgeID = Statement.generateId("Forum" + forum.getId());
        String schemaNodeID = Statement.generateId("S_Forum" + forum.getId());
        String nodeID = Statement.generateId("Forum" + forum.getId());

        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeNode(schemaNodeID, nodeID, (schema, node) -> {
                    schema.withNodeLabels("Forum")
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("id", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("title", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                            });


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
            String rightSchemaEdgeID = Statement.generateId("S_Tag" + tagId);
            String rightEdgeID = Statement.generateId("Tag" + tagId);
            writers.get(SOCIAL_NETWORK_ACTIVITY)
                    .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                        schema.as(leftSchemaEdgeID, Relationship.HAS_TAG.toString(), rightSchemaEdgeID);
                        edge.as(leftEdgeID, Relationship.HAS_TAG.toString(), rightEdgeID);
                    });
        }
    }

    public void serialize(final Post post) {
        String leftSchemaEdgeID = Statement.generateId("S_Post" + post.getMessageId());
        String leftEdgeID = Statement.generateId("Post" + post.getMessageId());
        String schemaNodeID = Statement.generateId("S_Post" + post.getMessageId());
        String nodeID = Statement.generateId("Post" + post.getMessageId());

        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeNode(schemaNodeID, nodeID, (schema, node) -> {
                    schema.withNodeLabels("Forum")
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("language", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING).asOptional()
                                );
                                propsSchemas.add("imageFile", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING).asOptional()
                                );
                            });


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
            String rightSchemaEdgeID = Statement.generateId("S_Tag" + tagId);
            String rightEdgeID = Statement.generateId("Tag" + tagId);

            writers.get(SOCIAL_NETWORK_ACTIVITY)
                    .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                        schema.as(leftSchemaEdgeID, Relationship.HAS_TAG.toString(), rightSchemaEdgeID);
                        edge.as(leftEdgeID, Relationship.HAS_TAG.toString(), rightEdgeID);
                    });
        }
    }

    public void serialize(final Comment comment) {
        String leftSchemaEdgeID = Statement.generateId("S_Comment" + comment.getRootPostId());
        String leftEdgeID = Statement.generateId("Comment" + comment.getRootPostId());

        String rightSchemaEdgeID = Statement.generateId("S_Comment" + comment.getMessageId());
        String rightEdgeID = Statement.generateId("Comment" + comment.getMessageId());

        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(leftSchemaEdgeID, Relationship.IS_REPLY_OF.toString(), rightSchemaEdgeID);
                    edge.as(leftEdgeID, Relationship.IS_REPLY_OF.toString(), rightEdgeID);
                });
    }

    public void serialize(final Photo photo) {
    }

    public void serialize(final ForumMembership membership) {
        String leftSchemaEdgeID = Statement.generateId("S_ForumMembership" + membership.getForumId());
        String leftEdgeID = Statement.generateId("ForumMembership" + membership.getForumId());

        String rightSchemaEdgeID = Statement.generateId("S_ForumMembership" + membership.getForumId());
        String rightEdgeID = Statement.generateId("ForumMembership" + membership.getForumId());

        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(leftSchemaEdgeID, Relationship.HAS_MEMBER.toString(), rightSchemaEdgeID)
                            .withPropsDefinition(schemasOfProperty -> schemasOfProperty.add("creationDate",
                                    propertySchema -> propertySchema.generateSchema(PrimitiveType.DATE_TIME)));

                    edge.as(leftEdgeID, Relationship.HAS_MEMBER.toString(), rightEdgeID)
                            .withProperties(properties -> {
                                properties.add("creationDate", property ->
                                        property.generatePrimitive(PrimitiveType.DATE_TIME, formatDateTime(membership.getCreationDate())));
                            });
                });
    }

    public void serialize(final Like like) {
        String leftSchemaEdgeID = Statement.generateId("S_Like" + like.getPerson());
        String leftEdgeID = Statement.generateId("Like" + like.getPerson());

        String rightSchemaEdgeID = Statement.generateId("S_Like" + like.getMessageId());
        String rightEdgeID = Statement.generateId("Like" + like.getMessageId());

        writers.get(SOCIAL_NETWORK_ACTIVITY)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(leftSchemaEdgeID, Relationship.LIKES.toString(), rightSchemaEdgeID)
                            .withPropsDefinition(schemasOfProperty -> schemasOfProperty.add("creationDate",
                                    propertySchema -> propertySchema.generateSchema(PrimitiveType.DATE_TIME)));

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

