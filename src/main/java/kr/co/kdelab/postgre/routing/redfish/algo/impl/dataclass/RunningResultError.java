package kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass;

import com.opencsv.bean.CsvBindByPosition;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import java.io.*;
import java.text.DateFormat;
import java.util.Date;

public class RunningResultError extends RunningResult {
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
    private String message;

    public String getStrStartTime() {
        return strStartTime;
    }

    public int getSource() {
        return source;
    }

    public int getTarget() {
        return target;
    }

    public String getAlgoTag() {
        return algoTag;
    }

    public long getCost() {
        return cost;
    }

    public long getTimeDelay() {
        return timeDelay;
    }

    public int getIterationX() {
        return iterationX;
    }

    public int getIterationY() {
        return iterationY;
    }

    public String getMessage() {
        return message;
    }

    public void setStrStartTime(String strStartTime) {
        this.strStartTime = strStartTime;
    }

    public void setSource(int source) {
        this.source = source;
    }

    public void setTarget(int target) {
        this.target = target;
    }

    public void setAlgoTag(String algoTag) {
        this.algoTag = algoTag;
    }

    public void setCost(long cost) {
        this.cost = cost;
    }

    public void setTimeDelay(long timeDelay) {
        this.timeDelay = timeDelay;
    }

    public void setIterationX(int iterationX) {
        this.iterationX = iterationX;
    }

    public void setIterationY(int iterationY) {
        this.iterationY = iterationY;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public RunningResultError(long start, long end, int source, int target, int iterationX, int iterationY, String message, String algoTag, long cost) {
        super(false);

        this.strStartTime = DateFormat.getDateTimeInstance().format(new Date(start));
        this.source = source;
        this.target = target;
        this.algoTag = algoTag;
        this.cost = cost;
        this.timeDelay = end - start;
        this.iterationX = iterationX;
        this.iterationY = iterationY;
        this.message = message;
    }

    // TODO: 18. 8. 17 해당 메소드에 대한 작업이 진행되지 않았음.
    public RunningResultError(Exception e) {
        super(false);

        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            e.printStackTrace(pw);
            message = sw.toString(); // stack trace as a string
        }
    }

    @Override
    public String toString() {
        return message + "\n";
    }


    public void writeCSV(String logFile) {
        try (Writer writer = new FileWriter(logFile, true)) {
            StatefulBeanToCsv<RunningResultError> beanToCsv = new StatefulBeanToCsvBuilder<RunningResultError>(writer).build();
            beanToCsv.write(this);
        } catch (CsvDataTypeMismatchException | CsvRequiredFieldEmptyException | IOException e) {
            e.printStackTrace();
        }
    }

}
