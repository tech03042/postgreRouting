package kr.co.kdelab.postgre.routing;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBUtil {
    public int getOnceByInteger(Statement statement, String sql) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next())
                return resultSet.getInt(1);
        }
        return -1;
    }

    public static long getOnceByLong(Statement statement, String sql) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next())
                return resultSet.getLong(1);
        }
        return -1;
    }

}
