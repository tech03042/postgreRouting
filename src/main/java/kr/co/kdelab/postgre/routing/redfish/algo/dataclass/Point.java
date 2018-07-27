package kr.co.kdelab.postgre.routing.redfish.algo.dataclass;

public class Point {
    private int source;
    private int target;

    public Point(int source, int target) {
        this.source = source;
        this.target = target;
    }

    public int getSource() {
        return source;
    }

    public int getTarget() {
        return target;
    }
}
