package kr.co.kdelab.postgre.routing.redfish.algo;

import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.sql.SQLException;

public abstract class ShortestPathOption {
    private ShortestPathOptionType shortestPathOptionType;

    ShortestPathOptionType getShortestPathOptionType() {
        return shortestPathOptionType;
    }

    public ShortestPathOption(ShortestPathOptionType shortestPathOptionType) {
        this.shortestPathOptionType = shortestPathOptionType;
    }

    public abstract void run(JDBConnectionInfo jdbConnectionInfo) throws Exception;

}
