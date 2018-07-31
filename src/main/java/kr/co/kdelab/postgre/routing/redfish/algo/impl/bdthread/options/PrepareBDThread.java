package kr.co.kdelab.postgre.routing.redfish.algo.impl.bdthread.options;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOption;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.sql.Connection;
import java.sql.Statement;

public class PrepareBDThread extends ShortestPathOption {


    private boolean usingTaIndex;

    public PrepareBDThread() {
        this(false);
    }

    public PrepareBDThread(boolean usingTaIndex) {
        super(ShortestPathOptionType.RUNNING_PRE);
        this.usingTaIndex = usingTaIndex;
    }

    @Override
    public void run(JDBConnectionInfo jdbConnectionInfo) throws Exception {
        try (Connection connection = jdbConnectionInfo.createConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE UNLOGGED TABLE IF NOT EXISTS ta (\n" +
                        "  nid int NOT NULL,\n" +
                        "  d2s int DEFAULT NULL,\n" +
                        "  p2s int DEFAULT NULL,\n" +
                        "  fwd int DEFAULT NULL,\n" +
                        "  f boolean DEFAULT FALSE,\n" +
                        "  PRIMARY KEY (nid)\n" +
                        ");");
                statement.execute("CREATE UNLOGGED TABLE IF NOT EXISTS ta2 (\n" +
                        "  nid int NOT NULL,\n" +
                        "  d2s int DEFAULT NULL,\n" +
                        "  p2s int DEFAULT NULL,\n" +
                        "  fwd int DEFAULT NULL,\n" +
                        "  f boolean DEFAULT FALSE,\n" +
                        "  PRIMARY KEY (nid)\n" +
                        ");\n");

                statement.execute("CREATE OR REPLACE VIEW te_b AS select te.tid AS fid,te.fid AS tid,te.cost AS cost from te;");

                if (usingTaIndex) {
                    statement.execute("CREATE INDEX ta_min_index ON ta USING btree(f, d2s)");
                    statement.execute("CREATE INDEX ta2_min_index ON ta2 USING btree(f, d2s)");
                }
            }
        }
    }
}
