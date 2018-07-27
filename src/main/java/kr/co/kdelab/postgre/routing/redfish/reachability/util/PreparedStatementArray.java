package kr.co.kdelab.postgre.routing.redfish.reachability.util;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

public class PreparedStatementArray extends ArrayList<PreparedStatement> {
    public void closeAll() {
        forEach(this::closeSilently);
    }

    private void closeSilently(PreparedStatement preparedStatement) {
        try {
            preparedStatement.close();
        } catch (SQLException ignored) {
        }
    }
}
