package kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass;

import java.text.DateFormat;
import java.util.Date;

public class RunningResultSuccess extends RunningResult {
    private String strStartTime;
    private int source;
    private int target;
    private String algoTag;
    private long cost;
    private long timeDelay;
    private int iterationX, iterationY;
    private String strPath;

    public RunningResultSuccess(long start, long end, int source, int target, int iterationX, int iterationY, String strPath, String algoTag, long cost) {
        super(true);

        this.strStartTime = DateFormat.getDateTimeInstance().format(new Date(start));
        this.source = source;
        this.target = target;
        this.algoTag = algoTag;
        this.cost = cost;
        this.timeDelay = end - start;
        this.iterationX = iterationX;
        this.iterationY = iterationY;
        this.strPath = strPath;
    }

    public void writeCSV() {

    }

    @Override
    public String toString() {
        return String.format("When started : %s\n", strStartTime)
                + String.format("Start = > Target : %d => %d\n", source, target)
                + String.format("Time delay : %d ms\n", timeDelay)
                + String.format("Iteration F, B = %d, %d\n", iterationX, iterationY)
                + String.format("Path = %s\n", strPath)
                + String.format("Cost = %d\n", cost)
                + String.format("Algorithm = %s\n", algoTag);
    }

}
