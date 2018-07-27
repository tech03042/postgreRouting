package kr.co.kdelab.postgre.routing.redfish.algo.impl.util.options;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOption;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.sql.Connection;
import java.sql.Statement;

public class TAClear extends ShortestPathOption {


    public TAClear(ShortestPathOptionType shortestPathOptionType) {
        super(shortestPathOptionType);
    }

    public TAClear() {
        super(ShortestPathOptionType.PRE_LOAD);
    }

    @Override
    public void run(JDBConnectionInfo jdbConnectionInfo) throws Exception {
        try (Connection connection = jdbConnectionInfo.createConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS ta; DROP TABLE IF EXISTS ta2;");
                statement.execute("DROP TABLE IF EXISTS ta0; DROP TABLE IF EXISTS ta1;");
            }
        }
    }
}
