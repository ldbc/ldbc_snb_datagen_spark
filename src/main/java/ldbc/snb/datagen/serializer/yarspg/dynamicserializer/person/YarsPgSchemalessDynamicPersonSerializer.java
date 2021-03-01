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
import ldbc.snb.datagen.serializer.yarspg.EdgeType;
import ldbc.snb.datagen.serializer.yarspg.Relationship;
import ldbc.snb.datagen.serializer.yarspg.Statement;
import ldbc.snb.datagen.serializer.yarspg.YarsPgSerializer;
import ldbc.snb.datagen.serializer.yarspg.property.ComplexType;
import ldbc.snb.datagen.serializer.yarspg.property.PrimitiveProperty;
import ldbc.snb.datagen.serializer.yarspg.property.PrimitiveType;
import ldbc.snb.datagen.util.DateUtils;

import java.util.List;
import java.util.stream.Collectors;

import static ldbc.snb.datagen.serializer.FileName.SOCIAL_NETWORK_PERSON;

public class YarsPgSchemalessDynamicPersonSerializer extends DynamicPersonSerializer<HdfsYarsPgWriter> implements YarsPgSerializer {

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(SOCIAL_NETWORK_PERSON);
    }

    @Override
    public void writeFileHeaders() {
    }

    @Override
    protected void serialize(final Person person) {
        String leftEdgeID = Statement.generateId("Person" + person.getAccountId());
        String nodeID = Statement.generateId("Person" + person.getAccountId());

        writers.get(SOCIAL_NETWORK_PERSON).writeNode(nodeID, (node) -> {
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
                                property.withPrimitive(
                                        new PrimitiveProperty(PrimitiveType.STRING, getGender(person.getGender())))
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

        String rightEdgeID = Statement.generateId("City" + person.getCityId());

        writers.get(SOCIAL_NETWORK_PERSON).writeEdge(EdgeType.DIRECTED, (edge) -> {
            edge.as(leftEdgeID, Relationship.IS_LOCATED_IN.toString(), rightEdgeID);
        });

        for (Integer interestIdx : person.getInterests()) {
            String rightEdgeInterestID = Statement.generateId("Tag" + interestIdx);
            writers.get(SOCIAL_NETWORK_PERSON).writeEdge(EdgeType.DIRECTED, (edge) -> {
                edge.as(leftEdgeID, Relationship.HAS_INTEREST.toString(), rightEdgeInterestID);
            });
        }
    }

    @Override
    protected void serialize(final StudyAt studyAt, Person person) {
        String leftEdgeID = Statement.generateId("Person" + person.getAccountId());
        String rightEdgeID = Statement.generateId("University" + studyAt.university);

        writers.get(SOCIAL_NETWORK_PERSON).writeEdge(EdgeType.DIRECTED, (edge) -> {
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
        String leftEdgeID = Statement.generateId("Person" + person.getAccountId());
        String rightEdgeID = Statement.generateId("Company" + workAt.company);

        writers.get(SOCIAL_NETWORK_PERSON).writeEdge(EdgeType.DIRECTED, (edge) -> {
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
        String leftEdgeID = Statement.generateId("Person" + person.getAccountId());
        String rightEdgeID = Statement.generateId("Person" + knows.to()
                .getAccountId());

        writers.get(SOCIAL_NETWORK_PERSON).writeEdge(EdgeType.DIRECTED, (edge) -> {
            edge.as(leftEdgeID, Relationship.KNOWS.toString(), rightEdgeID)
                    .withProperties(properties -> {
                        properties.add("creationDate", property ->
                                property.generatePrimitive(PrimitiveType.DATE_TIME, formatDateTime(knows.getCreationDate()))
                        );
                    });
        });
    }
}
