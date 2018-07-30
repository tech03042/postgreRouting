package kr.co.kdelab.postgre.routing.tangcori.impl;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOption;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.sql.Connection;
import java.sql.Statement;

public class PrepareSeoRBFS extends ShortestPathOption {
    public PrepareSeoRBFS() {
        super(ShortestPathOptionType.RUNNING_PRE);
    }

    @Override
    public void run(JDBConnectionInfo jdbConnectionInfo) throws Exception {
        try (Connection connection = jdbConnectionInfo.createConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS ta; DROP TABLE IF EXISTS ta2;");
                statement.execute("CREATE TABLE ta (\n" +
                        "  nid int NOT NULL,\n" +
                        "  d2s int DEFAULT NULL,\n" +
                        "  p2s int DEFAULT NULL,\n" +
                        "  fwd int DEFAULT NULL,\n" +
                        "  f boolean DEFAULT FALSE,\n" +
                        "  PRIMARY KEY (nid,d2s,p2s)\n" +
                        ");");
                statement.execute("CREATE TABLE ta2 (\n" +
                        "  nid int NOT NULL,\n" +
                        "  d2s int DEFAULT NULL,\n" +
                        "  p2s int DEFAULT NULL,\n" +
                        "  fwd int DEFAULT NULL,\n" +
                        "  f boolean DEFAULT FALSE,\n" +
                        "  PRIMARY KEY (nid,d2s,p2s)\n" +
                        ");");
            }
        }
    }
}
