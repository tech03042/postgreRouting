package kr.co.kdelab.postgre.routing.redfish.algo.impl.rbfs.options;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOption;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.sql.Connection;
import java.sql.Statement;

public class PrepareBiRbfs extends ShortestPathOption {
    public PrepareBiRbfs() {
        super(ShortestPathOptionType.RUNNING_PRE);
    }

    @Override
    public void run(JDBConnectionInfo jdbConnectionInfo) throws Exception {
        try (Connection connection = jdbConnectionInfo.createConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE UNLOGGED TABLE IF NOT EXISTS ta0(nid int PRIMARY KEY, d2s int, p2s int, fwd int); CREATE UNLOGGED TABLE IF NOT EXISTS ta1(nid int PRIMARY KEY, d2s int, p2s int, fwd int);");

                statement.execute("TRUNCATE ta0; TRUNCATE ta1;");
            }
        }
    }
}
