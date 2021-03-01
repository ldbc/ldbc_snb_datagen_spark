package ldbc.snb.datagen.serializer.yarspg.dynamicserializer.person;

import com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.entities.dynamic.relations.StudyAt;
import ldbc.snb.datagen.entities.dynamic.relations.WorkAt;
import ldbc.snb.datagen.hadoop.writer.HdfsYarsPgWriter;
import ldbc.snb.datagen.serializer.DynamicPersonSerializer;
import ldbc.snb.datagen.serializer.FileName;
import ldbc.snb.datagen.serializer.yarspg.*;
import ldbc.snb.datagen.serializer.yarspg.property.ComplexType;
import ldbc.snb.datagen.serializer.yarspg.property.PrimitiveType;
import ldbc.snb.datagen.util.DateUtils;

import java.util.List;
import java.util.stream.Collectors;

import static ldbc.snb.datagen.serializer.FileName.SOCIAL_NETWORK_PERSON;

public class YarsPgCanonicalDynamicPersonSerializer extends DynamicPersonSerializer<HdfsYarsPgWriter> implements YarsPgSerializer {

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(SOCIAL_NETWORK_PERSON);
    }

    @Override
    protected void addition() {
        writers.get(SOCIAL_NETWORK_PERSON).setCanonical(true);
    }

    @Override
    public void writeFileHeaders() {
        writers.get(SOCIAL_NETWORK_PERSON)
                .writeHeader(Header.NODE_SCHEMA.toString());
        writers.get(SOCIAL_NETWORK_PERSON)
                .writeHeader(Header.EDGE_SCHEMA.toString());
        writers.get(SOCIAL_NETWORK_PERSON)
                .writeHeader(Header.NODES.toString());
        writers.get(SOCIAL_NETWORK_PERSON)
                .writeHeader(Header.EDGES.toString());
    }

    @Override
    protected void serialize(final Person person) {
        String leftSchemaEdgeID = Statement.generateId("S_Person" + person.getAccountId());
        String leftEdgeID = Statement.generateId("Person" + person.getAccountId());
        String schemaNodeID = Statement.generateId("S_Person" + person.getAccountId());
        String nodeID = Statement.generateId("Person" + person.getAccountId());

        writers.get(SOCIAL_NETWORK_PERSON)
                .writeNode(schemaNodeID, nodeID, (schema, node) -> {
                    schema.withNodeLabels("Person")
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("id", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("firstName", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("lastName", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("gender", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("birthday", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.DATE)
                                );
                                propsSchemas.add("email", propSchema ->
                                        propSchema.generateSchema(1, ComplexType.LIST, PrimitiveType.STRING)
                                );
                                propsSchemas.add("speaks", propSchema ->
                                        propSchema.generateSchema(1, ComplexType.LIST, PrimitiveType.STRING)
                                );
                                propsSchemas.add("browserUsed", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                                propsSchemas.add("locationIP", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.STRING)
                                );
                            });


                    node.withNodeLabels("Person")
                            .withProperties(properties -> {
                                properties.add("id", property ->
                                        property.generatePrimitive(PrimitiveType.STRING,
                                                Long.toString(person.getAccountId()))
                                );
                                properties.add("firstName", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, person.getFirstName())
                                );
                                properties.add("lastName", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, person.getLastName())
                                );
                                properties.add("gender", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, getGender(person.getGender()))
                                );
                                properties.add("birthday", property ->
                                        property.generatePrimitive(PrimitiveType.DATE,
                                                formatDate(person.getBirthday()))
                                );
                                properties.add("email", property ->
                                        property.generateComplex(ComplexType.LIST,
                                                PrimitiveType.STRING,
                                                person.getEmails())
                                );

                                properties.add("speaks", property ->
                                        property.generateComplex(
                                                ComplexType.LIST, PrimitiveType.STRING,
                                                person.getLanguages()
                                                        .stream()
                                                        .map(l -> Dictionaries.languages.getLanguageName(l))
                                                        .collect(
                                                                Collectors.toList()))
                                );

                                properties.add("browserUsed", property ->
                                        property.generatePrimitive(PrimitiveType.STRING,
                                                Dictionaries.browsers.getName(
                                                        person.getBrowserId()))
                                );
                                properties.add("locationIP", property ->
                                        property.generatePrimitive(PrimitiveType.STRING, person.getIpAddress()
                                                .toString())
                                );
                            });
                });

        String rightSchemaEdgeID = Statement.generateId("S_City" + person.getCityId());
        String rightEdgeID = Statement.generateId("City" + person.getCityId());

        writers.get(SOCIAL_NETWORK_PERSON)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(leftSchemaEdgeID, Relationship.IS_LOCATED_IN.toString(), rightSchemaEdgeID);
                    edge.as(leftEdgeID, Relationship.IS_LOCATED_IN.toString(), rightEdgeID);
                });

        for (Integer interestIdx : person.getInterests()) {
            String rightSchemaEdgeInterestID = Statement.generateId("S_Tag" + interestIdx);
            String rightEdgeInterestID = Statement.generateId("Tag" + interestIdx);
            writers.get(SOCIAL_NETWORK_PERSON)
                    .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                        schema.as(leftSchemaEdgeID, Relationship.HAS_INTEREST.toString(), rightSchemaEdgeInterestID);
                        edge.as(leftEdgeID, Relationship.HAS_INTEREST.toString(), rightEdgeInterestID);
                    });
        }
    }

    @Override
    protected void serialize(final StudyAt studyAt, Person person) {
        String leftSchemaEdgeID = Statement.generateId("S_Person" + person.getAccountId());
        String leftEdgeID = Statement.generateId("Person" + person.getAccountId());
        String rightSchemaEdgeID = Statement.generateId("S_University" + studyAt.university);
        String rightEdgeID = Statement.generateId("University" + studyAt.university);

        writers.get(SOCIAL_NETWORK_PERSON)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(leftSchemaEdgeID, Relationship.STUDY_AT.toString(), rightSchemaEdgeID)
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("classYear", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.INTEGER)
                                );
                            });


                    edge.as(leftEdgeID, Relationship.STUDY_AT.toString(), rightEdgeID)
                            .withProperties(properties -> {
                                properties.add("classYear", property ->
                                        property.generatePrimitive(PrimitiveType.INTEGER,
                                                DateUtils.formatYear(studyAt.year))
                                );
                            });
                });
    }

    @Override
    protected void serialize(WorkAt workAt, Person person) {
        String leftSchemaEdgeID = Statement.generateId("S_Person" + person.getAccountId());
        String leftEdgeID = Statement.generateId("Person" + person.getAccountId());
        String rightSchemaEdgeID = Statement.generateId("S_Company" + workAt.company);
        String rightEdgeID = Statement.generateId("Company" + workAt.company);

        writers.get(SOCIAL_NETWORK_PERSON)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(leftSchemaEdgeID, Relationship.WORK_AT.toString(), rightSchemaEdgeID)
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("workFrom", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.INTEGER)
                                );
                            });


                    edge.as(leftEdgeID, Relationship.WORK_AT.toString(), rightEdgeID)
                            .withProperties(properties -> {
                                properties.add("workFrom", property ->
                                        property.generatePrimitive(PrimitiveType.INTEGER,
                                                Long.toString(workAt.company))
                                );
                            });
                });
    }

    @Override
    protected void serialize(final Person person, Knows knows) {
        String leftSchemaEdgeID = Statement.generateId("S_Person" + person.getAccountId());
        String leftEdgeID = Statement.generateId("Person" + person.getAccountId());
        String rightSchemaEdgeID = Statement.generateId("S_Person" + knows.to()
                .getAccountId());
        String rightEdgeID = Statement.generateId("Person" + knows.to()
                .getAccountId());

        writers.get(SOCIAL_NETWORK_PERSON)
                .writeEdge(EdgeType.DIRECTED, (schema, edge) -> {
                    schema.as(leftSchemaEdgeID, Relationship.KNOWS.toString(), rightSchemaEdgeID)
                            .withPropsDefinition(propsSchemas -> {
                                propsSchemas.add("creationDate", propSchema ->
                                        propSchema.generateSchema(PrimitiveType.DATE_TIME)
                                );
                            });


                    edge.as(leftEdgeID, Relationship.KNOWS.toString(), rightEdgeID)
                            .withProperties(properties -> {
                                properties.add("creationDate", property ->
                                        property.generatePrimitive(PrimitiveType.DATE_TIME,
                                                DateUtils.formatYear(knows.getCreationDate()))
                                );
                            });
                });
    }
}
