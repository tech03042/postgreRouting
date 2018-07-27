package kr.co.kdelab.postgre.routing.redfish.algo;

public enum ShortestPathOptionType {
    // 32 16 8 4 2 1
    USE_ONLY(0b00000),
    PRE_LOAD(0b00001),
    POST_LOAD(0b00010),
    RUNNING_PRE(0b00101),
    RUNNING_POST(0b00110);

    int val;

    ShortestPathOptionType(int val) {
        this.val = val;
    }

    public boolean isPreExecute() {
        return (val & 0b0001) != 0b0;
    }

    public boolean isPostExecute() {
        return (val & 0b0010) != 0b0;
    }

    public boolean isRunning() {
        return (val & 0b00100) != 0b0;
    }

}
