package kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass;

import com.opencsv.bean.CsvBindByPosition;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ExpandableRunningResult extends RunningResult {
    @CsvBindByPosition(position = 0)
    private String rbType;
    @CsvBindByPosition(position = 1)
    private String dataSet;
    @CsvBindByPosition(position = 2)
    private
    int source;

    @CsvBindByPosition(position = 3)
    private
    int target;

    @CsvBindByPosition(position = 4)
    private
    long taCountF;

    @CsvBindByPosition(position = 5)
    private
    long taCountB;

    @CsvBindByPosition(position = 6)
    private
    int xid;


    @CsvBindByPosition(position = 7)
    private
    int iterationX;
    @CsvBindByPosition(position = 8)
    private
    int iterationY;

    @CsvBindByPosition(position = 9)
    private
    long minCost0;
    @CsvBindByPosition(position = 10)
    private
    long minCost1;

    @CsvBindByPosition(position = 11)
    private
    long rbCount;

    @CsvBindByPosition(position = 12)
    private
    long timeRB; // 리쳐빌리티 속도
    @CsvBindByPosition(position = 13)
    private
    long timeDelay; // 알고리즘 속도

    @CsvBindByPosition(position = 14)
    private
    long sumTime; // timeRB + timeDelay

    @CsvBindByPosition(position = 15)
    private
    int pathSzF;
    @CsvBindByPosition(position = 16)
    private
    int pathSzB;

    @CsvBindByPosition(position = 17)
    private
    String pathF;
    @CsvBindByPosition(position = 18)
    private
    String pathB;

    public String getRbType() {
        return rbType;
    }

    public void setRbType(String rbType) {
        this.rbType = rbType;
    }

    public String getDataSet() {
        return dataSet;
    }

    public void setDataSet(String dataSet) {
        this.dataSet = dataSet;
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

    public long getTaCountF() {
        return taCountF;
    }

    public void setTaCountF(long taCountF) {
        this.taCountF = taCountF;
    }

    public long getTaCountB() {
        return taCountB;
    }

    public void setTaCountB(long taCountB) {
        this.taCountB = taCountB;
    }

    public int getXid() {
        return xid;
    }

    public void setXid(int xid) {
        this.xid = xid;
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

    public long getMinCost0() {
        return minCost0;
    }

    public void setMinCost0(long minCost0) {
        this.minCost0 = minCost0;
    }

    public long getMinCost1() {
        return minCost1;
    }

    public void setMinCost1(long minCost1) {
        this.minCost1 = minCost1;
    }

    public long getRbCount() {
        return rbCount;
    }

    public void setRbCount(long rbCount) {
        this.rbCount = rbCount;
    }

    public long getTimeRB() {
        return timeRB;
    }

    public void setTimeRB(long timeRB) {
        this.timeRB = timeRB;
    }

    public long getTimeDelay() {
        return timeDelay;
    }

    public void setTimeDelay(long timeDelay) {
        this.timeDelay = timeDelay;
    }

    public long getSumTime() {
        return sumTime;
    }

    public void setSumTime(long sumTime) {
        this.sumTime = sumTime;
    }

    public int getPathSzF() {
        return pathSzF;
    }

    public void setPathSzF(int pathSzF) {
        this.pathSzF = pathSzF;
    }

    public int getPathSzB() {
        return pathSzB;
    }

    public void setPathSzB(int pathSzB) {
        this.pathSzB = pathSzB;
    }

    public String getPathF() {
        return pathF;
    }

    public void setPathF(String pathF) {
        this.pathF = pathF;
    }

    public String getPathB() {
        return pathB;
    }

    public void setPathB(String pathB) {
        this.pathB = pathB;
    }


    public ExpandableRunningResult(Connection connection, int source, int target, int xid, int iterationX, int iterationY, long minCost0, long minCost1, long timeDelay, int pathSzF, int pathSzB, String pathF, String pathB) throws SQLException {
        super(true);

        this.source = source;
        this.target = target;
        this.xid = xid;
        this.iterationX = iterationX;
        this.iterationY = iterationY;
        this.minCost0 = minCost0;
        this.minCost1 = minCost1;
        this.timeDelay = timeDelay;
        this.pathSzF = pathSzF;
        this.pathSzB = pathSzB;
        this.pathF = pathF;
        this.pathB = pathB;


        try (Statement statement = connection.createStatement()) {
            this.taCountF = getOnce(statement, "SELECT count(*) FROM ta");
            this.taCountB = getOnce(statement, "SELECT count(*) FROM ta2");
            this.rbCount = getOnce(statement, "SELECT count(*) FROM rb");
        }
    }

    public long getOnce(Statement statement, String sql) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next())
                return resultSet.getInt(1);
        }
        return -1;
    }

    public ExpandableRunningResult(boolean isSuccess) {
        super(isSuccess);
    }

    @Override
    public String toString(String dataSet) {
        return super.toString(dataSet);
    }

    public void writeCSV(String logFile, String rbType, String dataSet, long timeRB) throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
        this.rbType = rbType;
        this.dataSet = dataSet;
        this.timeRB = timeRB;
        sumTime = timeRB + timeDelay;

        try (Writer writer = new FileWriter(logFile, true)) {
            StatefulBeanToCsv<ExpandableRunningResult> beanToCsv = new StatefulBeanToCsvBuilder<ExpandableRunningResult>(writer).build();
            beanToCsv.write(this);
        }
    }
}
