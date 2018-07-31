package kr.co.kdelab.postgre.routing.redfish.algo;

import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

public abstract class ShortestPathOption implements Closeable {
    private int source;
    private int target;

    private ShortestPathOptionType shortestPathOptionType;

    ShortestPathOptionType getShortestPathOptionType() {
        return shortestPathOptionType;
    }

    public ShortestPathOption(ShortestPathOptionType shortestPathOptionType) {
        this.shortestPathOptionType = shortestPathOptionType;
    }

    public abstract void run(JDBConnectionInfo jdbConnectionInfo) throws Exception;

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

    @Override
    public void close() throws IOException {

    }
}
