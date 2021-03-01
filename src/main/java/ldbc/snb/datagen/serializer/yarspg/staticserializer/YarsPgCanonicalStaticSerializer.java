package ldbc.snb.datagen.serializer.yarspg.staticserializer;

import com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.statictype.Organisation;
import ldbc.snb.datagen.entities.statictype.TagClass;
import ldbc.snb.datagen.entities.statictype.place.Place;
import ldbc.snb.datagen.entities.statictype.tag.Tag;
import ldbc.snb.datagen.hadoop.writer.HdfsYarsPgWriter;
import ldbc.snb.datagen.serializer.FileName;
import ldbc.snb.datagen.serializer.StaticSerializer;
import ldbc.snb.datagen.serializer.yarspg.*;
import ldbc.snb.datagen.serializer.yarspg.property.PrimitiveType;
import ldbc.snb.datagen.vocabulary.DBP;

import java.util.List;

import static ldbc.snb.datagen.serializer.FileName.SOCIAL_NETWORK_STATIC;

public class YarsPgCanonicalStaticSerializer extends StaticSerializer<HdfsYarsPgWriter> implements YarsPgSerializer {

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(SOCIAL_NETWORK_STATIC);
    }

    @Override
    protected void addition() {
        writers.get(SOCIAL_NETWORK_STATIC).setCanonical(true);
    }

    @Override
    public void writeFileHeaders() {
        writers.get(SOCIAL_NETWORK_STATIC)
                .writeHeader(Header.NODE_SCHEMA.toString());
        writers.get(SOCIAL_NETWORK_STATIC)
                .writeHeader(Header.EDGE_SCHEMA.toString());
        writers.get(SOCIAL_NETWORK_STATIC)
                .writeHeader(Header.NODES.toString());
        writers.get(SOCIAL_NETWORK_STATIC)
                .writeHeader(Header.EDGES.toString());
    }

    public void serialize(final Place place) {
        String schemaNodeID = Statement.generateId("S_Place" + place.getId());
        String nodeID = Statement.generateId("Place" + place.getId());

        writers.get(SOCIAL_NETWORK_STATIC)
                .writeNode(schemaNodeID, nodeID, (schema, node) -> {
                    schema.withNodeLabels("Place")
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("id", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("name", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("url", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                            });


                    node.withNodeLabels("Place")
                            .withProperties(properties -> {
                                properties.add("id", property ->
                                        property.generatePrimitive(PrimitiveType.STRING,
                                                Long.toString(place.getId()))
                                );
                                properties.add("name", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, place.getName())
                                );
                                properties.add("url", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, DBP.getUrl(place.getName()))
                                );
                            });
                });

        if (place.getType().equals(Place.CITY) || place.getType().equals(Place.COUNTRY)) {
            String placeSchemaEdgeID = Statement.generateId("S_Place" + place.getId());
            String placeEdgeID = Statement.generateId("Place" + place.getId());
            String belongsToSchemaEdgeID = Statement.generateId("S_Place" + Dictionaries.places.belongsTo(place.getId()));
            String belongsToEdgeID = Statement.generateId("Place" + Dictionaries.places.belongsTo(place.getId()));
            writers.get(SOCIAL_NETWORK_STATIC)
                    .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                        schema.as(placeSchemaEdgeID, Relationship.IS_PART_OF.toString(), belongsToSchemaEdgeID);
                        edge.as(placeEdgeID, Relationship.IS_PART_OF.toString(), belongsToEdgeID);
                    });
        }
    }

    public void serialize(final Organisation organisation) {
        String schemaNodeID = Statement.generateId("S_Organisation" + organisation.id);
        String nodeID = Statement.generateId("Organisation" + organisation.id);

        writers.get(SOCIAL_NETWORK_STATIC)
                .writeNode(schemaNodeID, nodeID, (schema, node) -> {
                    schema.withNodeLabels("Organisation")
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("id", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("name", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("url", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                            });


                    node.withNodeLabels("Organisation")
                            .withProperties(properties -> {
                                properties.add("id", property ->
                                        property.generatePrimitive(PrimitiveType.STRING,
                                                Long.toString(organisation.id))
                                );
                                properties.add("name", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, organisation.name)
                                );
                                properties.add("url", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, DBP.getUrl(organisation.name))
                                );
                            });
                });
    }

    public void serialize(final TagClass tagClass) {
        String schemaNodeID = Statement.generateId("S_TagClass" + tagClass.id);
        String nodeID = Statement.generateId("TagClass" + tagClass.id);

        writers.get(SOCIAL_NETWORK_STATIC)
                .writeNode(schemaNodeID, nodeID, (schema, node) -> {
                    schema.withNodeLabels("TagClass")
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("id", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("name", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("url", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                            });


                    node.withNodeLabels("TagClass")
                            .withProperties(properties -> {
                                properties.add("id", property ->
                                        property.generatePrimitive(PrimitiveType.STRING,
                                                Long.toString(tagClass.id))
                                );
                                properties.add("name", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, tagClass.name)
                                );
                                properties.add("url", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, DBP.getUrl(tagClass.name))
                                );
                            });
                });

        String leftSchemaEdgeID = Statement.generateId("S_TagClass" + tagClass.id);
        String leftEdgeID = Statement.generateId("TagClass" + tagClass.id);

        if (tagClass.parent != -1) {
            String rightSchemaEdgeID = Statement.generateId("S_TagClass" + tagClass.parent);
            String rightEdgeID = Statement.generateId("TagClass" + tagClass.parent);

            writers.get(SOCIAL_NETWORK_STATIC)
                    .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                        schema.as(leftSchemaEdgeID, Relationship.IS_SUBCLASS_OF.toString(), rightSchemaEdgeID);
                        edge.as(leftEdgeID, Relationship.IS_SUBCLASS_OF.toString(), rightEdgeID);
                    });
        }
    }

    public void serialize(final Tag tag) {
        String schemaNodeID = Statement.generateId("S_Tag" + tag.id);
        String nodeID = Statement.generateId("Tag" + tag.id);

        writers.get(SOCIAL_NETWORK_STATIC)
                .writeNode(schemaNodeID, nodeID, (schema, node) -> {
                    schema.withNodeLabels("Tag")
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("id", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("name", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("url", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                            });


                    node.withNodeLabels("Tag")
                            .withProperties(properties -> {
                                properties.add("id", property ->
                                        property.generatePrimitive(PrimitiveType.STRING,
                                                Long.toString(tag.id))
                                );
                                properties.add("name", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, tag.name)
                                );
                                properties.add("url", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, DBP.getUrl(tag.name))
                                );
                            });
                });

        String tagSchemaEdgeID = Statement.generateId("S_Tag" + tag.id);
        String tagEdgeID = Statement.generateId("Tag" + tag.id);

        String tagClassSchemaEdgeID = Statement.generateId("S_TagClass" + tag.tagClass);
        String tagClassEdgeID = Statement.generateId("TagClass" + tag.tagClass);

        writers.get(SOCIAL_NETWORK_STATIC)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(tagSchemaEdgeID, Relationship.HAS_TYPE.toString(), tagClassSchemaEdgeID);
                    edge.as(tagEdgeID, Relationship.HAS_TYPE.toString(), tagClassEdgeID);
                });
    }


    @Override
    protected boolean isDynamic() {
        return false;
    }

}
