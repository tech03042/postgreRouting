package kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass;

public class FrontierResult {
    private int nodeId;
    private int cost;

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public int getCost() {
        return cost;
    }

    public void setCost(int cost) {
        this.cost = cost;
    }

    public FrontierResult(int nodeId, int cost) {
        this.nodeId = nodeId;
        this.cost = cost;
    }
}
