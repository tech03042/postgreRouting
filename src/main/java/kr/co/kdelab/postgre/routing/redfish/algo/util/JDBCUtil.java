package kr.co.kdelab.postgre.routing.redfish.algo.util;

import java.sql.ResultSet;
import java.sql.SQLException;

public class JDBCUtil {
    public static long getLong(ResultSet resultSet) throws SQLException {
        if (resultSet.next())
            return resultSet.getLong(1);
        return 0;
    }
}
