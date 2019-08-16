package de.hhu.bsinfo.dx_graph_preprocessing.model;

public class Partition {
    private long startByteOffset;
    private long endByteOffset;
    private long startVertex;
    private long endVertex;
    private short nodeId;

    public Partition(long startByteOffset, long endByteOffset, long startVertex, long endVertex, short nodeId) {
        this.startByteOffset = startByteOffset;
        this.endByteOffset = endByteOffset;
        this.startVertex = startVertex;
        this.endVertex = endVertex;
        this.nodeId = nodeId;
    }

    public Partition(long startByteOffset, short nodeId) {
        this.startByteOffset = startByteOffset;
        this.nodeId = nodeId;
    }

    public void setEndByteOffset(long endByteOffset) {
        this.endByteOffset = endByteOffset;
    }

    public long getStartByteOffset() {
        return startByteOffset;
    }

    public long getEndByteOffset() {
        return endByteOffset;
    }

    public void setStartVertex(long startVertex) {
        this.startVertex = startVertex;
    }

    public void setEndVertex(long endVertex) {
        this.endVertex = endVertex;
    }

    public long getStartVertex() {
        return startVertex;
    }

    public long getEndVertex() {
        return endVertex;
    }

    public short getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Partition of Node %d:\n", nodeId))
                .append(String.format("Startoffset: %d\n", startByteOffset))
                .append(String.format("Endoffset: %d\n", endByteOffset))
                .append(String.format("Startvertex: %d\n", startVertex))
                .append(String.format("Endvertex: %d", endVertex));

        return sb.toString();
    }
}
