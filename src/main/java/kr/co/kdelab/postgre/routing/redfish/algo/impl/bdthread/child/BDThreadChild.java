package kr.co.kdelab.postgre.routing.redfish.algo.impl.bdthread.child;

import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.MergeResult;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class BDThreadChild extends Thread {

    public BDThreadChild(JDBConnectionInfo jdbConnectionInfo, String tableNameTA, String tableNameTE) {
        this.jdbConnectionInfo = jdbConnectionInfo;
        this.tableNameTA = tableNameTA;
        this.tableNameTE = tableNameTE;
    }

    private String tableNameTA;
    private String tableNameTE;
    private boolean isTermination = false;

    private JDBConnectionInfo jdbConnectionInfo;

    private int iteration, dist, affected = Integer.MAX_VALUE;

    private int lastCheckIteration = 0;

    public int getIteration() {
        return iteration;
    }

    public int getDist() {
        return dist;
    }

    public int getAffected() {
        return affected;
    }

    public boolean isTermination() {
        return isTermination;
    }

    public void terminate() {
        isTermination = true;
    }

    private MergeResult p_mergeResult = new MergeResult(0, 0);

    public String getTableNameTA() {
        return tableNameTA;
    }

    public String getTableNameTE() {
        return tableNameTE;
    }

    public abstract String getFEMSql();

    @Override
    public void run() {
        super.run();

        try (Connection connection = jdbConnectionInfo.createConnection()) {
            PreparedStatement checkAllCheckedFF = connection.prepareStatement("SELECT nid FROM " + tableNameTA + " WHERE f=false limit 1");
            PreparedStatement femStmt = connection.prepareStatement(
                    getFEMSql());
            while (!isTermination) {
                MergeResult mergeResult = fem(femStmt, iteration);

                if (mergeResult != null) {
                    if (mergeResult.affected != 0)
                        affected = mergeResult.affected;
                    else {
                        affected = getUnFFCount(checkAllCheckedFF);
                        if (affected == 0) {
                            isTermination = true;
                        }
                    }

                }
                if ((mergeResult != null ? mergeResult.affected : 0) != 0)
                    dist = mergeResult.minCost;


                iteration++;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        isTermination = true;
    }

    private int getUnFFCount(PreparedStatement preparedStatement) throws SQLException {
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next())
                return 1;
        }
        return 0;
    }


    private MergeResult fem(PreparedStatement femStmt, int iteration) throws SQLException {
        femStmt.setInt(1, iteration + 1);

        // F, E, M Operator with 1 SQL
        try (ResultSet resultSet = femStmt.executeQuery()) {
            if (resultSet.next()) {
                int affected = resultSet.getInt(1);
                int minCost = resultSet.getInt(2);

                p_mergeResult.affected = affected;
                p_mergeResult.minCost = minCost;
                return p_mergeResult;
            }
        }
        return null;
    }

    public int getLastCheckIteration() {
        return lastCheckIteration;
    }

    public void setLastCheckIteration(int lastCheckIteration) {
        this.lastCheckIteration = lastCheckIteration;
    }

}
