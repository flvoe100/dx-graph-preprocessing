package de.hhu.bsinfo.dx_graph_preprocessing.model;

public class Partition {
    private long startByteOffset;
    private long endByteOffset;
    private short nodeId;

    public Partition(long startByteOffset, long endByteOffset, short nodeId) {
        this.startByteOffset = startByteOffset;
        this.endByteOffset = endByteOffset;
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

    public short getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Partition of Node %d:\n", nodeId))
                .append(String.format("Startoffset: %d\n", startByteOffset))
                .append(String.format("Endoffset: %d\n", endByteOffset));
        return sb.toString();
    }
}
