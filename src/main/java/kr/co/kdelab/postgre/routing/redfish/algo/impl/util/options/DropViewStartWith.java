package kr.co.kdelab.postgre.routing.redfish.algo.impl.util.options;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOption;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class DropViewStartWith extends ShortestPathOption {
    private String prefix;

    public DropViewStartWith(ShortestPathOptionType shortestPathOptionType, String prefix) {
        super(shortestPathOptionType);
        this.prefix = prefix;
    }

    @Override
    public void run(JDBConnectionInfo jdbConnectionInfo) throws Exception {
        try (Connection connection = jdbConnectionInfo.createConnection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                try (Statement deleteStatement = connection.createStatement()) {
                    try (ResultSet resultSet = statement.executeQuery("select table_name from INFORMATION_SCHEMA.views where table_schema='" + jdbConnectionInfo.getSchema() + "' and table_name like '" + prefix + "%'")) {
                        while (resultSet.next())
                            deleteStatement.execute("DROP VIEW IF EXISTS " + resultSet.getString(1) + " CASCADE");
                    }
                }
            }
            connection.commit();
        }
    }
}
