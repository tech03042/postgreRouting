package kr.co.kdelab.postgre.routing.parser.internal;

public class TestSetFormat {
    public TestSetFormat(int nodeId, int targetId, double weight) {
        this.nodeId = nodeId;
        this.targetId = targetId;
        this.weight = weight;
    }

    private int nodeId;
    private int targetId;
    private double weight;

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public int getTargetId() {
        return targetId;
    }

    public void setTargetId(int targetId) {
        this.targetId = targetId;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    @Override
    public String toString() {
        return String.format("%d %d %f", nodeId, targetId, weight);
    }
}
