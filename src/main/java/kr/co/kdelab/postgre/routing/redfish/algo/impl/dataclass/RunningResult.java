package kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass;

public class RunningResult {
    private boolean isSuccess;

    public RunningResult(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public String toString(String dataSet) {
        return toString() + String.format("DataSet = %s\n", dataSet);
    }
}
