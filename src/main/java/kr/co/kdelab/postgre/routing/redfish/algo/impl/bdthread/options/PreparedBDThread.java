package kr.co.kdelab.postgre.routing.redfish.algo.impl.bdthread.options;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOption;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.sql.Connection;
import java.sql.Statement;

public class PreparedBDThread extends ShortestPathOption {
    public PreparedBDThread() {
        super(ShortestPathOptionType.PRE_LOAD);
    }

    @Override
    public void run(JDBConnectionInfo jdbConnectionInfo) throws Exception {
        try (Connection connection = jdbConnectionInfo.createConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("TRUNCATE ta");
                statement.execute("TRUNCATE ta2");

                statement.execute("CREATE OR REPLACE VIEW te_b AS select te.tid AS fid,te.fid AS tid,te.cost AS cost from te");
            }
        }
    }
}
