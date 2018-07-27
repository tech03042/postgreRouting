package kr.co.kdelab.postgre.routing.redfish.algo.impl.util.options;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOption;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class DropTableStartWith extends ShortestPathOption {
    private String prefix;

    public DropTableStartWith(boolean isPreLoad, String prefix) {
        super(isPreLoad ? ShortestPathOptionType.PRE_LOAD : ShortestPathOptionType.POST_LOAD);
        this.prefix = prefix;
    }

    @Override
    public void run(JDBConnectionInfo jdbConnectionInfo) throws Exception {
        try (Connection connection = jdbConnectionInfo.createConnection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                try (Statement deleteStatement = connection.createStatement()) {
                    try (ResultSet resultSet = statement.executeQuery("select table_name from INFORMATION_SCHEMA.tables where table_schema='" + jdbConnectionInfo.getSchema() + "' and table_name like '" + prefix + "%' and table_type='BASE TABLE'")) {
                        while (resultSet.next())
                            deleteStatement.execute("DROP TABLE IF EXISTS " + resultSet.getString(1) + " CASCADE");
                    }
                }
            }
            connection.commit();
        }
    }
}
