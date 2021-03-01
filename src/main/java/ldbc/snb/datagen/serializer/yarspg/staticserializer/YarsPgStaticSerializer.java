package ldbc.snb.datagen.serializer.yarspg.staticserializer;

import com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.statictype.Organisation;
import ldbc.snb.datagen.entities.statictype.TagClass;
import ldbc.snb.datagen.entities.statictype.place.Place;
import ldbc.snb.datagen.entities.statictype.tag.Tag;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.hadoop.writer.HdfsYarsPgWriter;
import ldbc.snb.datagen.serializer.FileName;
import ldbc.snb.datagen.serializer.StaticSerializer;
import ldbc.snb.datagen.serializer.csv.CsvSerializer;
import ldbc.snb.datagen.serializer.yarspg.EdgeType;
import ldbc.snb.datagen.serializer.yarspg.Relationship;
import ldbc.snb.datagen.serializer.yarspg.Statement;
import ldbc.snb.datagen.serializer.yarspg.YarsPgSerializer;
import ldbc.snb.datagen.serializer.yarspg.property.PrimitiveType;
import ldbc.snb.datagen.vocabulary.DBP;
import ldbc.snb.datagen.vocabulary.DBPOWL;

import java.util.List;

import static ldbc.snb.datagen.serializer.FileName.*;

public class YarsPgStaticSerializer extends StaticSerializer<HdfsYarsPgWriter> implements YarsPgSerializer {

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(SOCIAL_NETWORK_STATIC);
    }

    @Override
    public void writeFileHeaders() {
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

        String leftSchemaEdgeID = Statement.generateId("S_Tag" + tag.id);
        String leftEdgeID = Statement.generateId("Tag" + tag.id);

        String rightSchemaEdgeID = Statement.generateId("S_Tag" + tag.tagClass);
        String rightEdgeID = Statement.generateId("Tag" + tag.tagClass);

        writers.get(SOCIAL_NETWORK_STATIC)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(leftSchemaEdgeID, Relationship.HAS_TYPE.toString(), rightSchemaEdgeID);
                    edge.as(leftEdgeID, Relationship.HAS_TYPE.toString(), rightEdgeID);
                });
    }


    @Override
    protected boolean isDynamic() {
        return false;
    }

}