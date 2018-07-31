package kr.co.kdelab.postgre.routing;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBUtil {
    public int getOnceByInteger(Statement statement, String sql) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next())
                return resultSet.getInt(1);
        }
        return 0;
    }

    public static long getOnceByLong(Statement statement, String sql) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next())
                return resultSet.getLong(1);
        }
        return 0;
    }

    public static long getOnceByLong(PreparedStatement preparedStatement) throws SQLException {
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next())
                return resultSet.getLong(1);
        }
        return 0;
    }

    public int getOnceByInteger(PreparedStatement preparedStatement) throws SQLException {
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next())
                return resultSet.getInt(1);
        }
        return 0;
    }
}
