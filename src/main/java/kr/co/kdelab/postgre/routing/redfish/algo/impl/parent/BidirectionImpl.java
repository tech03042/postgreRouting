package kr.co.kdelab.postgre.routing.redfish.algo.impl.parent;

import kr.co.kdelab.postgre.routing.DBUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public interface BidirectionImpl {
    int FORWARD = 0;
    int BACKWARD = 1;

    default long getMinCost(PreparedStatement statement) throws SQLException {
        long value = DBUtil.getOnceByLong(statement);
        if (value == 0)
            return Long.MAX_VALUE;
        return value;
    }


    default long getMinCost(PreparedStatement[] stmtEquallyExpandedCost, int direction, int X, int Y) throws SQLException {
        stmtEquallyExpandedCost[direction].setInt(1, X);
        stmtEquallyExpandedCost[direction].setInt(2, Y);

        long value = DBUtil.getOnceByLong(stmtEquallyExpandedCost[direction]);
        if (value == 0)
            return Long.MAX_VALUE;
        return value;
    }


    default int getMidNode(Connection connection, long min_cost) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("select TF.nid from ta as TF, ta2 as TB where TF.nid = TB.nid and TF.d2s+TB.d2s = ? limit 1")) {
            statement.setLong(1, min_cost);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next())
                    return resultSet.getInt(1);
            }
        }
        return -1;
    }


    default List<Integer> extractPath(Connection connection, int mid, int direction) throws SQLException {
        return extractPath(connection, mid, direction, false);
    }

    default List<Integer> extractPath(Connection connection, int mid, int direction, boolean isRBFS) throws SQLException {
        ArrayList<Integer> path = new ArrayList<>();
        int p2s = mid;
        path.add(p2s);

        String sql = "select p2s from ta" + (direction == FORWARD ? "" : "2") + " where nid = ?";
        if (isRBFS)
            sql = String.format("select p2s from ta%s where nid = ?", direction == FORWARD ? "0" : "1");

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            while (true) {
                preparedStatement.setInt(1, p2s);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        int temp = resultSet.getInt(1);
                        if (temp == p2s)
                            break;
                        p2s = temp;
                    } else
                        break;
                }
                path.add(p2s);
            }
        }
        if (direction == 0)
            Collections.reverse(path);

        return path;
    }

}
