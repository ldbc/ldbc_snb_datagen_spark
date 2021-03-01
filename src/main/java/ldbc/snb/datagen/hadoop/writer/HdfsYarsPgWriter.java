package ldbc.snb.datagen.hadoop.writer;

import ldbc.snb.datagen.serializer.yarspg.*;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.HashSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class HdfsYarsPgWriter extends HdfsWriter {
    private final StringBuffer buffer;
    private boolean isCanonical = false;
    private Node node;
    private NodeSchema nodeSchema;
    private Edge edge;
    private EdgeSchema edgeSchema;
    private HashSet<String> edgeSchemaKey = new HashSet<>();
    private HashSet<String> nodeSchemaKey = new HashSet<>();

    public HdfsYarsPgWriter(FileSystem fs, String outputDir, String prefix, int numPartitions, boolean compressed) throws IOException {
        super(fs, outputDir, prefix, numPartitions, compressed, "yarspg");
        this.buffer = new StringBuffer();
    }

    public void setCanonical(boolean canonical) {
        isCanonical = canonical;
    }

    @Override
    public void close() {
        // Due tu keeping data in the buffer in canonical case,
        // data is saved just before closing the file
        if (isCanonical) {
            this.write(buffer.toString());
        }

        super.close();
    }

    public void writeHeader(String header) {
        if (isCanonical) {
            buffer.append(header)
                    .append("\n");
            return;
        }

        buffer.setLength(0);
        buffer.append(header)
                .append("\n");

        this.write(buffer.toString());
    }

    public void writeNode(String nodeID, Consumer<Node> consumer) {
        buffer.setLength(0);
        node = new Node(nodeID);
        consumer.accept(node);

        buffer.append(node)
                .append("\n");

        this.write(buffer.toString());
    }

    public void writeEdge(EdgeType edgeType, Consumer<Edge> edgeConsumer) {
        buffer.setLength(0);
        edge = new Edge(edgeType);
        edgeConsumer.accept(edge);

        buffer.append(edge)
                .append("\n");

        this.write(buffer.toString());
    }

    public void writeNode(String schemaNodeID, String nodeID, BiConsumer<NodeSchema, Node> biConsumer) {
        if (!isCanonical)
            buffer.setLength(0);

        nodeSchema = new NodeSchema(schemaNodeID);
        node = new Node(nodeID);
        biConsumer.accept(nodeSchema, node);

        if (isCanonical) {
            // Make sure that specific schema is written only once
            // Workaround to `PersonExporter` implementation
            if (!nodeSchemaKey.contains(nodeSchema.getNodeLabel()
                    .get(0))) {
                int afterNodeSchemaHeaderIndex = buffer.indexOf(String.valueOf(
                        Header.NODE_SCHEMA)) + Header.NODE_SCHEMA.toString()
                        .length() + 1;

                buffer.insert(afterNodeSchemaHeaderIndex, nodeSchema + "\n");
            }

            nodeSchemaKey.add(nodeSchema.getNodeLabel()
                    .get(0));

            int afterNodesHeaderIndex = buffer.indexOf(String.valueOf(
                    Header.NODES)) + Header.NODES.toString()
                    .length() + 1;

            buffer.insert(afterNodesHeaderIndex, node + "\n");

            return;
        }

        // Make sure that specific schema is written only once
        // Workaround to `PersonExporter` implementation
        if (!nodeSchemaKey.contains(nodeSchema.getNodeLabel()
                .get(0))) {
            buffer.append(nodeSchema)
                    .append("\n");
        }
        nodeSchemaKey.add(nodeSchema.getNodeLabel()
                .get(0));

        buffer.append(node)
                .append("\n");

        this.write(buffer.toString());
    }

    public void writeEdge(EdgeType edgeType, BiConsumer<EdgeSchema, Edge> biConsumer) {
        if (!isCanonical)
            buffer.setLength(0);

        edge = new Edge(edgeType);
        edgeSchema = new EdgeSchema(edgeType);
        biConsumer.accept(edgeSchema, edge);

        if (isCanonical) {
            // Make sure that specific schema is written only once
            // Workaround to `PersonExporter` implementation
            if (!edgeSchemaKey.contains(edgeSchema.getEdgeLabel())) {
                int afterEdgeSchemaHeaderIndex = buffer.indexOf(String.valueOf(
                        Header.EDGE_SCHEMA)) + Header.EDGE_SCHEMA.toString()
                        .length() + 1;

                buffer.insert(afterEdgeSchemaHeaderIndex, edgeSchema + "\n");
            }

            int afterEdgesHeaderIndex = buffer.indexOf(String.valueOf(
                    Header.EDGES)) + Header.EDGES.toString()
                    .length() + 1;

            edgeSchemaKey.add(edgeSchema.getEdgeLabel());

            buffer.insert(afterEdgesHeaderIndex, edge + "\n");

            return;
        }

        // Make sure that specific schema is written only once
        // Workaround to `PersonExporter` implementation
        if (!edgeSchemaKey.contains(edgeSchema.getEdgeLabel())) {
            buffer.append(edgeSchema)
                    .append("\n");
        }
        edgeSchemaKey.add(edgeSchema.getEdgeLabel());

        buffer.append(edge)
                .append("\n");

        this.write(buffer.toString());
    }
}
