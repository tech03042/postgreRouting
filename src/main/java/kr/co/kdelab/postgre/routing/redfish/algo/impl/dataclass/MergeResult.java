package kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass;

public class MergeResult {
    public int affected;
    public int minCost;

    public MergeResult(int affected, int minCost) {
        this.affected = affected;
        this.minCost = minCost;
    }
}