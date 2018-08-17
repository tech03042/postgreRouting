package kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass;

import com.opencsv.bean.CsvBindByPosition;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import kr.co.kdelab.postgre.routing.redfish.reachability.dataclass.RechabilityResult;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.DateFormat;
import java.util.Date;

public class RunningResultSuccess extends RunningResult {
    @CsvBindByPosition(position = 0)
    private String strStartTime;
    @CsvBindByPosition(position = 1)
    private int source;
    @CsvBindByPosition(position = 2)
    private int target;
    @CsvBindByPosition(position = 3)
    private String algoTag;
    @CsvBindByPosition(position = 4)
    private long cost;
    @CsvBindByPosition(position = 5)
    private long timeDelay;
    @CsvBindByPosition(position = 6)
    private int iterationX;
    @CsvBindByPosition(position = 7)
    private int iterationY;
    @CsvBindByPosition(position = 8)
    private String strPath;


    public String getStrStartTime() {
        return strStartTime;
    }

    public void setStrStartTime(String strStartTime) {
        this.strStartTime = strStartTime;
    }

    public int getSource() {
        return source;
    }

    public void setSource(int source) {
        this.source = source;
    }

    public int getTarget() {
        return target;
    }

    public void setTarget(int target) {
        this.target = target;
    }

    public String getAlgoTag() {
        return algoTag;
    }

    public void setAlgoTag(String algoTag) {
        this.algoTag = algoTag;
    }

    public long getCost() {
        return cost;
    }

    public void setCost(long cost) {
        this.cost = cost;
    }

    public long getTimeDelay() {
        return timeDelay;
    }

    public void setTimeDelay(long timeDelay) {
        this.timeDelay = timeDelay;
    }

    public int getIterationX() {
        return iterationX;
    }

    public void setIterationX(int iterationX) {
        this.iterationX = iterationX;
    }

    public int getIterationY() {
        return iterationY;
    }

    public void setIterationY(int iterationY) {
        this.iterationY = iterationY;
    }

    public String getStrPath() {
        return strPath;
    }

    public void setStrPath(String strPath) {
        this.strPath = strPath;
    }

    private RechabilityResult rechabilityResult = null;

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

    public RechabilityResult getRechabilityResult() {
        return rechabilityResult;
    }

    public void setRechabilityResult(RechabilityResult rechabilityResult) {
        this.rechabilityResult = rechabilityResult;
    }

    @Override
    public String toString() {
        String ret = String.format("When started : %s\n", strStartTime)
                + String.format("Start = > Target : %d => %d\n", source, target)
                + String.format("Time delay : %d ms\n", timeDelay)
                + String.format("Iteration F, B = %d, %d\n", iterationX, iterationY)
                + String.format("Path = %s\n", strPath)
                + String.format("Cost = %d\n", cost)
                + String.format("Algorithm = %s\n", algoTag);
        if (rechabilityResult != null)
            ret += rechabilityResult.toString() + "\n";
        return ret;
    }

    public void writeCSV(String logFile) {
        try (Writer writer = new FileWriter(logFile, true)) {
            StatefulBeanToCsv<RunningResultSuccess> beanToCsv = new StatefulBeanToCsvBuilder<RunningResultSuccess>(writer).build();
            beanToCsv.write(this);
        } catch (CsvDataTypeMismatchException | CsvRequiredFieldEmptyException | IOException e) {
            e.printStackTrace();
        }
    }




}
